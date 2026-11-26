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
  * With `sortedSplits` (`sorted_agg_splits`), input that comes in non-decreasing order of an
  * integer grouping key is instead cut at key changes into contiguous runs ([[SortedAggSplits]]),
  * each aggregated as a partition without hashing or scattering its rows; the output is then in key
  * order.
  *
  * With `sizeByRows`, a partition of an aggregation whose estimate reached
  * [[GroupTable.MaxPresizeGroups]] sizes its tables for its row count, up to that cap.
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

  /** Whether the partitions' fused folds are column-major (`column_major_fused_sums`) */
  var columnMajorSums: Boolean = true

  /** Set [[columnMajorSums]] */
  def withColumnMajorSums(enabled: Boolean): this.type =
    columnMajorSums = enabled
    this

  /** Whether the partitions group a single Int64 key through a [[LongGroupTable]] */
  var longGroupTables: Boolean = true

  /** Set [[longGroupTables]] */
  def withLongGroupTables(enabled: Boolean): this.type =
    longGroupTables = enabled
    this

  /**
    * Whether the partitions group runs of non-decreasing single Int64 keys without hashing them
    * (`sorted_group_runs`). Partials of a scan sorted by the key arrive in part order, so each
    * partition's rows keep the order
    */
  var sortedGroupRuns: Boolean = true

  /** Set [[sortedGroupRuns]] */
  def withSortedGroupRuns(enabled: Boolean): this.type =
    sortedGroupRuns = enabled
    this

  /**
    * Whether the partitions build their output with fewer passes and copies (`lean_agg_results`,
    * [[HashAggOp.leanResults]])
    */
  var leanResults: Boolean = true

  /** Set [[leanResults]] */
  def withLeanResults(enabled: Boolean): this.type =
    leanResults = enabled
    this

  /**
    * Whether input sorted by an integer grouping key is split at key changes into contiguous runs
    * aggregated as the partitions, instead of hashing and scattering its rows (`sorted_agg_splits`,
    * [[SortedAggSplits]])
    */
  var sortedSplits: Boolean = true

  /** Set [[sortedSplits]] */
  def withSortedSplits(enabled: Boolean): this.type =
    sortedSplits = enabled
    this

  /**
    * Whether hash-routed input with packable keys is routed by one packed key
    * (`packed_agg_routing`): the routing tasks pack the keys from their ranges over the whole input
    * and copy only the packed key and the aggregates' columns into the partitions, which group by
    * the packed key without packing their rows again. Otherwise every column of the partitions'
    * rows is copied, and each partition packs its keys from its own ranges
    */
  var packedRouting: Boolean = true

  /** Set [[packedRouting]] */
  def withPackedRouting(enabled: Boolean): this.type =
    packedRouting = enabled
    this

  /**
    * Whether a partition sizes its group table and aggregate state for its exact row count (at most
    * [[GroupTable.MaxPresizeGroups]]), when the aggregation's group estimate reached that cap
    * (`size_partitions_by_rows`). A partition has at most one group per row, and a capped estimate
    * split over the partitions sized each for a fraction of its groups (4096 of ~50K for TPC-DS q97
    * and q78 at SF10), so the table rehashed and the state grew several times
    */
  var sizeByRows: Boolean = true

  /** Set [[sizeByRows]] */
  def withSizeByRows(enabled: Boolean): this.type =
    sizeByRows = enabled
    this

  /** Whether the last aggregation routed its rows by a packed key (for tests) */
  private[engine] var usedPackedRouting: Boolean = false

  /** Whether the last aggregation split its input at sorted key changes (for tests) */
  private[engine] var usedSortedSplits: Boolean = false

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
    // The packed key column appended to the input columns
    val packedIn = Schema(child.schema.fields :+ Field("__packed_key", PType.Int64))
    val packedAt = ColRef(child.schema.size, PType.Int64, "__packed_key")
    // A partition has at most one group per row. With `routed`, the batches hold the packed key of
    // that packing as one more column
    def aggregateAll(
        batches: Seq[Batch],
        groups: Int,
        routed: Option[AggKeyPacking] = None
    ): Seq[Batch] =
      val rows = batches.map(_.size).sum
      // A capped estimate gives no upper bound below the rows. The cap bounds the zero-filled
      // memory of an overestimate per partition, as it does for one table
      val presize =
        if sizeByRows && expectedGroups >= GroupTable.MaxPresizeGroups then
          math.min(rows, GroupTable.MaxPresizeGroups)
        else
          math.min(groups, rows)
      def run(in: Schema, bs: Seq[Batch], ks: IndexedSeq[VExpr], out: Schema): Seq[Batch] =
        val op = HashAggOp(BatchesOp(in, bs), ks, aggs, out, presize)
          // The keys are packed here from the partition's exact ranges, when they can be
          .withKeyOptions(pack = false, longDecimals = longDecimalKeys)
          .withFusedAccumulators(fuseAccumulators)
          .withColumnMajorSums(columnMajorSums)
          .withLongGroupTables(longGroupTables)
          .withSortedGroupRuns(sortedGroupRuns)
          .withLeanResults(leanResults)
        try op.drain()
        finally op.close()
      val keyVecs =
        if routed.isEmpty && packKeys && AggKeyPacking.packable(keys.map(_.ptype), longDecimalKeys)
        then
          batches.map(b => (keys.map(_.eval(b)).toArray, b.size))
        else
          Nil
      val packing =
        if routed.isDefined then
          routed
        else if keyVecs.isEmpty then
          None
        else
          AggKeyPacking.of(keys.map(_.ptype), keyVecs, longDecimalKeys)
      packing match
        case Some(packing) =>
          // The packed key as one more input column, whose groups unpack into the key columns
          val packed =
            if routed.isDefined then
              batches
            else
              batches
                .zip(keyVecs)
                .map((b, kv) => Batch(b.columns :+ packing.pack(kv._1, b.size), b.size))
          val out = Schema(Field("__packed_key", PType.Int64) +: schema.fields.drop(keys.size))
          run(packedIn, packed, IndexedSeq(packedAt), out).map { b =>
            val k = packing.unpack(b.columns(0).asInstanceOf[LongVec], b.size, leanResults)
            Batch(k ++ b.columns.drop(1), b.size)
          }
        case None =>
          run(child.schema, batches, keys, schema)
    end aggregateAll
    if total < minParallelRows || runner.parallelism <= 1 then
      return aggregateAll(input.map(_.compact()), expectedGroups).iterator
    val parts  = GroupTable.partitionCount(runner.parallelism)
    val splits =
      if sortedSplits then
        SortedAggSplits.of(input, keys, parts)
      else
        None
    splits match
      case Some(runs) =>
        // Each run holds every row of its groups, and the runs come in key order
        usedSortedSplits = true
        val handles = runs.map { r =>
          val rows   = r.iterator.map(input(_).size.toLong).sum
          val groups = (expectedGroups.toLong * rows / math.max(1L, total)).toInt
          runner.submit(() => aggregateAll(r.map(i => input(i).compact()), groups))
        }
        return handles.iterator.flatMap(_.get())
      case None =>
    val parallelRuns = parallelPartitioning && input.size > 1
    // Contiguous runs of input batches, about two per thread, routed as tasks
    val runs: IndexedSeq[IndexedSeq[Batch]] =
      if parallelRuns then
        val n    = math.min(input.size, runner.parallelism * 2)
        val step = (input.size + n - 1) / n
        input.grouped(step).toIndexedSeq
      else
        IndexedSeq(input)
    def eachRun[A](f: IndexedSeq[Batch] => A): IndexedSeq[A] =
      if parallelRuns then
        runs.map(run => runner.submit(() => f(run))).map(_.get())
      else
        runs.map(f)
    val types = keys.map(_.ptype)
    // The packing of the keys over the whole input, for routing by the packed key. Only column
    // keys: they read the same values in both passes, without evaluating an expression twice
    val packing: Option[AggKeyPacking] =
      if packedRouting && packKeys && keys.forall(_.isInstanceOf[ColRef]) &&
        AggKeyPacking.packable(types, longDecimalKeys)
      then
        val ranges = eachRun { run =>
          AggKeyPacking.ranges(
            types,
            run.map(b => (keys.map(_.eval(b.compact())).toArray, b.size)),
            longDecimalKeys
          )
        }
        if ranges.forall(_.isDefined) then
          AggKeyPacking.ofRanges(types, ranges.flatten)
        else
          None
      else
        None
    usedPackedRouting = packing.isDefined
    // The columns a partition reads when grouping by the packed key: the aggregates' and the key
    val routedColumns = (PartitionedAggOp.columnRefs(aggs) + packedAt.index).toArray.sorted
    // The rows of each input batch in each partition
    def route(batches: IndexedSeq[Batch]): Array[mutable.ArrayBuffer[Batch]] =
      val hasher =
        if packing.isDefined then
          GroupTable(IndexedSeq(PType.Int64), 0, false)
        else
          GroupTable(types, 0, longDecimalKeys)
      val out = Array.fill(parts)(mutable.ArrayBuffer.empty[Batch])
      batches.foreach { deferred =>
        // Copying each partition's rows from compact columns is cheaper than composing every
        // partition's positions over the deferred columns' sources, and than reading them in place
        // at the positions
        val b = deferred.compact()
        packing match
          case Some(pk) =>
            val key  = pk.pack(keys.map(_.eval(b)).toArray, b.size)
            val idx  = GroupTable.partitionRows(hasher.keyHashes(Array(key), b.size), b.size, parts)
            val wide = Batch(b.columns :+ key, b.size)
            var p    = 0
            while p < parts do
              if idx(p).nonEmpty then
                // The columns no aggregate reads stay deferred, never read. The partition task that
                // reads t starts after this routing task finished
                val t = wide.take(idx(p), idx(p).length)
                routedColumns.foreach(t.column)
                out(p) += t
              p += 1
          case None =>
            val h   = hasher.keyHashes(keys.map(_.eval(b)).toArray, b.size)
            val idx = GroupTable.partitionRows(h, b.size, parts)
            var p   = 0
            while p < parts do
              if idx(p).nonEmpty then
                out(p) += b.gather(idx(p), idx(p).length)
              p += 1
      }
      out
    end route
    val routed  = eachRun(route)
    val handles = (0 until parts).map { p =>
      val ps = routed.flatMap(_(p))
      runner.submit(() => aggregateAll(ps, expectedGroups / parts, packing))
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
  /** The input columns that the arguments and FILTERs of `aggs` reference */
  private[exec] def columnRefs(aggs: IndexedSeq[AggCall]): Set[Int] =
    val refs                  = Set.newBuilder[Int]
    def visit(e: VExpr): Unit =
      e match
        case c: ColRef =>
          refs += c.index
        case _ =>
          e.children.foreach(visit)
    aggs.foreach(a => (a.args ++ a.filter).foreach(visit))
    refs.result()

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
