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

/**
  * Splits each input batch by the hash of its first `numKeys` columns into the `parts` partitions
  * of a [[PartitionedAggOp]] (`route_partials_in_parts`): each output batch holds the rows of one
  * input batch that fall in one partition, in input order, with the key hash appended as an Int32
  * column ([[PartitionRouteOp.HashColumn]]). Partitions follow in partition order within each input
  * batch, and empty ones are skipped.
  *
  * It runs inside the parts of a scan pipeline, after the partial aggregates, so that the rows are
  * routed while the partials are still being computed, instead of after every part has finished.
  */
class PartitionRouteOp(val child: Operator, numKeys: Int, val parts: Int, longDecimalKeys: Boolean)
    extends Operator:
  require(numKeys > 0, "PartitionRouteOp needs grouping keys")
  require(Integer.bitCount(parts) == 1, s"Partition count must be a power of two: ${parts}")

  override val schema: Schema = Schema(
    child.schema.fields :+ Field(PartitionRouteOp.HashColumn, PType.Int32)
  )

  override def name: String            = "PartitionRoute"
  override def children: Seq[Operator] = Seq(child)

  private val keyTypes = child.schema.fields.take(numKeys).map(_.ptype).toIndexedSeq
  private val hasher   = GroupTable(keyTypes, 0, longDecimalKeys)

  // The pieces of the current input batch not emitted yet
  private var pending: Array[Batch] = null
  private var nextPiece             = 0

  def next(): Batch =
    while pending == null || nextPiece >= pending.length do
      val b = child.next()
      if b == null then
        return null
      pending = route(b)
      nextPiece = 0
    val piece = pending(nextPiece)
    pending(nextPiece) = null
    nextPiece += 1
    piece

  /** The non-empty partitions of `deferred`, each with the key hashes of its rows */
  private def route(deferred: Batch): Array[Batch] =
    val b = deferred.compact()
    val n = b.size
    if n == 0 then
      return Array.empty
    val keys = new Array[Vec](numKeys)
    var k    = 0
    while k < numKeys do
      keys(k) = b.column(k)
      k += 1
    val h     = hasher.keyHashes(keys, n)
    val shift = 32 - Integer.numberOfTrailingZeros(parts)
    // Every row in one partition (e.g. a small batch): no gather
    val first = h(0) >>> shift
    var i     = 1
    while i < n && (h(i) >>> shift) == first do
      i += 1
    if i == n then
      return Array(b ++ Batch(Array[Vec](IntVec(PType.Int32, h, null)), n))
    val idx = GroupTable.partitionRows(h, n, parts)
    val out = Array.newBuilder[Batch]
    var p   = 0
    while p < parts do
      val rows = idx(p)
      if rows.nonEmpty then
        val m  = rows.length
        val hs = new Array[Int](m)
        var j  = 0
        while j < m do
          hs(j) = h(rows(j))
          j += 1
        out += b.gather(rows, m) ++ Batch(Array[Vec](IntVec(PType.Int32, hs, null)), m)
      p += 1
    out.result()

  end route

end PartitionRouteOp

object PartitionRouteOp:
  /** The name of the key hash column appended to the routed batches */
  val HashColumn = "__key_hash"

  /** The partition of a routed batch, from the key hash of its first row */
  def partitionOf(b: Batch, parts: Int): Int =
    val h = b.column(b.numColumns - 1).asInstanceOf[IntVec]
    h.values(0) >>> (32 - Integer.numberOfTrailingZeros(parts))
