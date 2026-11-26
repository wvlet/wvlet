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
  * Groups streamed batches by one Int64-backed key column (`long_group_tables`): the packed keys of
  * [[PackedGroupTable]] and of partitioned final aggregations, and single Int64 keys.
  *
  * Each slot is a 4-byte group id, half a [[GroupTable]] slot (hash and group id), so a probe of a
  * near-unique key that ends at an empty slot touches half the memory; an occupied slot compares
  * the group's key. Hashes are recomputed from the keys, not stored per row or per group, and a new
  * key goes into the empty slot its probe ended at, with no second probe. NULL keys are one group
  * kept outside the slots.
  *
  * Group ids and their order are the same as a [[GroupTable]] over the key gives.
  */
final class LongGroupTable(keyType: PType, expectedGroups: Int = 0) extends GroupIndex:
  import LongGroupTable.*

  private val store = GroupTable.KeyStore(keyType).asInstanceOf[GroupTable.LongStore]
  // The group id of each slot, or Empty
  private var cap   = GroupTable.slotCapacity(expectedGroups)
  private var slots = emptySlots(cap)
  private var mask  = cap - 1
  // The key of each group in id order, and whether it is the NULL group
  private var values    = new Array[Long](math.max(16, math.min(expectedGroups, 1 << 20)))
  private var count     = 0
  private var nullGroup = -1

  def numGroups: Int = count

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    val v   = store.normalize(keys(0)).asInstanceOf[LongVec]
    val x   = v.values
    val ns  = v.nulls
    val ids = new Array[Int](n)
    if ns == null then
      var i = 0
      while i < n do
        ids(i) = idOf(x(i))
        i += 1
    else
      var i = 0
      while i < n do
        ids(i) =
          if ns(i) then
            nullId()
          else
            idOf(x(i))
        i += 1
    ids

  private def nullId(): Int =
    if nullGroup < 0 then
      nullGroup = add(0L)
    nullGroup

  /** The group of key k, added as a new group when it is not in the table */
  private def idOf(k: Long): Int =
    var idx = hash(k) & mask
    while true do
      val g = slots(idx)
      if g == Empty then
        val id = add(k)
        slots(idx) = id
        if count * 2 > cap then
          grow()
        return id
      if values(g) == k then
        return g
      idx = (idx + 1) & mask
    -1

  /** A new group of key k (0 for the NULL group), not yet in any slot */
  private def add(k: Long): Int =
    val g = count
    if g == values.length then
      values = java.util.Arrays.copyOf(values, g * 2)
    values(g) = k
    count += 1
    g

  /** Twice the slots, with every non-NULL group re-inserted in id order */
  private def grow(): Unit =
    cap *= 2
    mask = cap - 1
    slots = emptySlots(cap)
    var g = 0
    while g < count do
      if g != nullGroup then
        val k   = values(g)
        var idx = hash(k) & mask
        while slots(idx) != Empty do
          idx = (idx + 1) & mask
        slots(idx) = g
      g += 1

  def keyVectors(): Array[Vec] =
    val nulls =
      if nullGroup < 0 then
        null
      else
        val a = new Array[Boolean](count)
        a(nullGroup) = true
        a
    Array(LongVec(keyType, java.util.Arrays.copyOf(values, count), nulls))

end LongGroupTable

object LongGroupTable:
  private inline val Empty = -1

  /** Whether keys of these types can be grouped by a [[LongGroupTable]] */
  def supports(keyTypes: IndexedSeq[PType]): Boolean =
    keyTypes.size == 1 && (
      keyTypes(0) match
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
          true
        case _ =>
          false
    )

  /** The hash a [[GroupTable]] gives one Int64 key */
  private inline def hash(k: Long): Int = GroupTable.fmix(GroupTable.hashLong(k))

  private def emptySlots(cap: Int): Array[Int] =
    val a = new Array[Int](cap)
    java.util.Arrays.fill(a, Empty)
    a

end LongGroupTable
