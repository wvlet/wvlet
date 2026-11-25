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

import wvlet.lang.engine.vector.*

/**
  * Groups streamed batches by a key of several integer, date, or decimal columns
  * ([[AggKeyPacking.packable]]) through one packed Int64 key, whose ranges come from the rows seen
  * so far: a [[GroupTable]] over the packed key takes its single-Int64 path.
  *
  * The first batch sets the ranges, with room for values about one range beyond each end. A batch
  * with a key outside them repacks the groups so far into ranges that cover it too, and once the
  * codes don't fit in 63 bits, the groups move to a [[GroupTable]] over the key columns. Either way
  * the groups are re-added in id order, so their ids, and the accumulators indexed by them, stay
  * valid. Group ids and their order are the same as a [[GroupTable]] over the key columns gives.
  */
final class PackedGroupTable(
    keyTypes: IndexedSeq[PType],
    expectedGroups: Int = 0,
    longDecimals: Boolean = false
) extends GroupIndex:
  require(AggKeyPacking.packable(keyTypes, longDecimals), s"Keys can't be packed: ${keyTypes}")

  private var packing: Option[AggKeyPacking] = None
  // Groups by the packed key while packing is defined
  private var packed: GroupTable = null
  // Groups by the key columns once they don't pack
  private var unpacked: GroupTable = null

  /** How many times the groups were repacked into wider ranges (for tests) */
  private[exec] var repacks: Int = 0

  /** Whether the groups are still keyed by packed values (for tests) */
  private[exec] def isPacked: Boolean = unpacked == null

  def numGroups: Int =
    if unpacked != null then
      unpacked.numGroups
    else if packed != null then
      packed.numGroups
    else
      0

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    if unpacked != null then
      return unpacked.groupIds(keys, n)
    val p =
      if packing.isEmpty then
        null
      else
        packing.get.tryPack(keys, n)
    if p != null then
      packed.groupIds(Array(p), n)
    else
      repack(keys, n)
      groupIds(keys, n)

  /** Re-add the groups so far keyed to cover the first n rows of keys too */
  private def repack(keys: Array[Vec], n: Int): Unit =
    val count   = numGroups
    val current =
      if packed == null then
        null
      else
        packing.get.unpack(packed.keyVectors()(0).asInstanceOf[LongVec], count)
    val presize = math.max(expectedGroups, count * 2)
    AggKeyPacking.covering(keyTypes, Option(current).map((_, count)), keys, n, longDecimals) match
      case Some(next) =>
        val table = GroupTable(IndexedSeq(PType.Int64), presize)
        if count > 0 then
          table.groupIds(Array(next.pack(current, count)), count)
          repacks += 1
        packing = Some(next)
        packed = table
      case None =>
        val table = GroupTable(keyTypes, presize, longDecimals)
        if count > 0 then
          table.groupIds(current, count)
        packing = None
        packed = null
        unpacked = table

  end repack

  def keyVectors(): Array[Vec] =
    if unpacked != null then
      unpacked.keyVectors()
    else if packed == null then
      GroupTable(keyTypes, 0, longDecimals).keyVectors()
    else
      packing.get.unpack(packed.keyVectors()(0).asInstanceOf[LongVec], packed.numGroups)

end PackedGroupTable

object PackedGroupTable:
  /**
    * A [[PackedGroupTable]] when `pack` and the keys can be packed, and a [[GroupTable]] over the
    * key columns otherwise, keyed by the codes of string keys that carry them with `stableCodes`
    */
  def forKeys(
      keyTypes: IndexedSeq[PType],
      expectedGroups: Int,
      pack: Boolean,
      longDecimals: Boolean,
      stableCodes: Boolean = false
  ): GroupIndex =
    if pack && AggKeyPacking.packable(keyTypes, longDecimals) then
      new PackedGroupTable(keyTypes, expectedGroups, longDecimals)
    else
      GroupTable(keyTypes, expectedGroups, longDecimals, stableCodes, packCodes = pack)

end PackedGroupTable
