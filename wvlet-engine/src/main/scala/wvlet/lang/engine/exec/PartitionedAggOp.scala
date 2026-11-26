/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import scala.collection.mutable

/**
  * Hash aggregation by grouping keys in parallel: input rows are split into partitions by the hash
  * of their key, so that every group falls in one partition, and each partition is aggregated as a
  * task of `runner`. Output: the groups of partition 0, then of partition 1, and so on.
  *
  * Inputs of fewer than `minParallelRows` rows are aggregated on the calling thread, in the input's
  * group order, as [[HashAggOp]] does. `expectedGroups` (0 when unknown) pre-sizes the group
  * tables, split across the partitions.
  *
  * With `parallelPartitioning`, input rows are routed to partitions as tasks too: each task hashes
  * and scatters a contiguous run of input batches, and each partition joins the tasks' rows in
  * input order, so its rows (and its groups' first-seen order) are the same as routing them on the
  * calling thread.
  *
  * With `packKeys`, each partition whose key columns are integers or dates that fit in one Int64
  * groups by the packed key ([[AggKeyPacking]], from the ranges of the partition's rows) and
  * unpacks the key columns of its groups.
  */
class PartitionedAggOp(
    val child: Operator,
    val keys: IndexedSeq[VExpr],
    val aggs: IndexedSeq[AggCall],
    val schema: Schema,
    runner: TaskRunner,
    minParallelRows: Int = PartitionedAggOp.MinParallelRows,
    val expectedGroups: Int = 0,
    parallelPartitioning: Boolean = true,
    val packKeys: Boolean = false
) extends Operator:
  require(keys.nonEmpty, "PartitionedAggOp needs grouping keys")

  override def name: String            = s"PartitionedAgg[parallelism=${runner.parallelism}]"
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(keys=[${keys.map(_.sig).mkString(", ")}], aggs=[${aggs.map(_.sig).mkString(", ")}])"

  private var result: Iterator[Batch] = null

  /** Whether decimal keys are grouped and packed as their unscaled Long values */
  var longDecimalKeys: Boolean = true

  /** Set [[longDecimalKeys]] */
  def withLongDecimalKeys(enabled: Boolean): this.type =
    longDecimalKeys = enabled
    this

  /** Whether the partitions' hash aggregations fuse their accumulators (`fused_accumulators`) */
  var fuseAccumulators: Boolean = true

  /** Set [[fuseAccumulators]] */
  def withFusedAccumulators(enabled: Boolean): this.type =
    fuseAccumulators = enabled
    this

  /** Whether the partitions group a single Int64 key through a [[LongGroupTable]] */
  var longGroupTables: Boolean = true

  /** Set [[longGroupTables]] */
  def withLongGroupTables(enabled: Boolean): this.type =
    longGroupTables = enabled
    this

  def next(): Batch =
    if result == null then
      result = aggregate()
    if result.hasNext then
      result.next()
    else
      null

  private def aggregate(): Iterator[Batch] =
    // Deferred columns are gathered by the tasks that route the batches
    val input = readInput()
    val total = input.map(_.size).sum
    // A partition has at most one group per row
    def aggregateAll(batches: Seq[Batch], groups: Int): Seq[Batch] =
      val rows = batches.map(_.size).sum
      def run(in: Schema, bs: Seq[Batch], ks: IndexedSeq[VExpr], out: Schema): Seq[Batch] =
        val op = HashAggOp(BatchesOp(in, bs), ks, aggs, out, math.min(groups, rows))
          // The keys are packed here from the partition's exact ranges, when they can be
          .withKeyOptions(pack = false, longDecimals = longDecimalKeys)
          .withFusedAccumulators(fuseAccumulators)
          .withLongGroupTables(longGroupTables)
        try op.drain()
        finally op.close()
      val keyVecs =
        if packKeys && AggKeyPacking.packable(keys.map(_.ptype), longDecimalKeys) then
          batches.map(b => (keys.map(_.eval(b)).toArray, b.size))
        else
          Nil
      val packing =
        if keyVecs.isEmpty then
          None
        else
          AggKeyPacking.of(keys.map(_.ptype), keyVecs, longDecimalKeys)
      packing match
        case Some(packing) =>
          // The packed key as one more input column, whose groups unpack into the key columns
          val w      = child.schema.size
          val in     = Schema(child.schema.fields :+ Field("__packed_key", PType.Int64))
          val packed = batches
            .zip(keyVecs)
            .map((b, kv) => Batch(b.columns :+ packing.pack(kv._1, b.size), b.size))
          val out = Schema(Field("__packed_key", PType.Int64) +: schema.fields.drop(keys.size))
          run(in, packed, IndexedSeq(ColRef(w, PType.Int64, "__packed_key")), out).map { b =>
            val k = packing.unpack(b.columns(0).asInstanceOf[LongVec], b.size)
            Batch(k ++ b.columns.drop(1), b.size)
          }
        case None =>
          run(child.schema, batches, keys, schema)
    end aggregateAll
    if total < minParallelRows || runner.parallelism <= 1 then
      return aggregateAll(input.map(_.compact()), expectedGroups).iterator
    val parts = GroupTable.partitionCount(runner.parallelism)
    // The rows of each input batch in each partition
    def route(batches: IndexedSeq[Batch]): Array[mutable.ArrayBuffer[Batch]] =
      val hasher = GroupTable(keys.map(_.ptype), 0, longDecimalKeys)
      val out    = Array.fill(parts)(mutable.ArrayBuffer.empty[Batch])
      batches.foreach { deferred =>
        // Copying each partition's rows from compact columns is cheaper than composing every
        // partition's positions over the deferred columns' sources
        val b   = deferred.compact()
        val h   = hasher.keyHashes(keys.map(_.eval(b)).toArray, b.size)
        val idx = GroupTable.partitionRows(h, b.size, parts)
        var p   = 0
        while p < parts do
          if idx(p).nonEmpty then
            out(p) += b.gather(idx(p), idx(p).length)
          p += 1
      }
      out
    val routed: Seq[Array[mutable.ArrayBuffer[Batch]]] =
      if parallelPartitioning && input.size > 1 then
        // Contiguous runs of input batches, about two per thread, routed as tasks
        val runs = math.min(input.size, runner.parallelism * 2)
        val step = (input.size + runs - 1) / runs
        input.grouped(step).toIndexedSeq.map(run => runner.submit(() => route(run))).map(_.get())
      else
        Seq(route(input))
    val handles = (0 until parts).map { p =>
      val ps = routed.flatMap(_(p))
      runner.submit(() => aggregateAll(ps, expectedGroups / parts))
    }
    handles.iterator.flatMap(_.get())

  end aggregate

  /** Every input batch, in order, with deferred columns left deferred (see [[Batch.take]]) */
  private def readInput(): IndexedSeq[Batch] =
    val b = IndexedSeq.newBuilder[Batch]
    var x = child.next()
    while x != null do
      b += x
      x = child.next()
    b.result()

end PartitionedAggOp

object PartitionedAggOp:
  /**
    * Below this many input rows, partitioning costs more than it saves. The
    * wvlet.engine.partitionedAggMinRows system property overrides it (tests use it to exercise the
    * parallel path on small data)
    */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.partitionedAggMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)
