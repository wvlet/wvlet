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
  * group order, as [[HashAggOp]] does.
  */
class PartitionedAggOp(
    val child: Operator,
    val keys: IndexedSeq[VExpr],
    val aggs: IndexedSeq[AggCall],
    val schema: Schema,
    runner: TaskRunner,
    minParallelRows: Int = PartitionedAggOp.MinParallelRows
) extends Operator:
  require(keys.nonEmpty, "PartitionedAggOp needs grouping keys")

  override def name: String            = s"PartitionedAgg[parallelism=${runner.parallelism}]"
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(keys=[${keys.map(_.sig).mkString(", ")}], aggs=[${aggs.map(_.sig).mkString(", ")}])"

  private var result: Iterator[Batch] = null

  def next(): Batch =
    if result == null then
      result = aggregate()
    if result.hasNext then
      result.next()
    else
      null

  private def aggregate(): Iterator[Batch] =
    val input                                         = child.drain()
    val total                                         = input.map(_.size).sum
    def aggregateAll(batches: Seq[Batch]): Seq[Batch] =
      val op = HashAggOp(BatchesOp(child.schema, batches), keys, aggs, schema)
      try op.drain()
      finally op.close()
    if total < minParallelRows || runner.parallelism <= 1 then
      return aggregateAll(input).iterator
    val parts      = GroupTable.partitionCount(runner.parallelism)
    val hasher     = GroupTable(keys.map(_.ptype))
    val partitions = Array.fill(parts)(mutable.ArrayBuffer.empty[Batch])
    input.foreach { b =>
      val h   = hasher.keyHashes(keys.map(_.eval(b)).toArray, b.size)
      val idx = GroupTable.partitionRows(h, b.size, parts)
      var p   = 0
      while p < parts do
        if idx(p).nonEmpty then
          partitions(p) += b.gather(idx(p), idx(p).length)
        p += 1
    }
    val handles = partitions.toIndexedSeq.map(ps => runner.submit(() => aggregateAll(ps.toSeq)))
    handles.iterator.flatMap(_.get())

  end aggregate

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
