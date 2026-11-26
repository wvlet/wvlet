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
  *
  * With `sortedRuns` (`sorted_group_runs`), the table starts without slots while the keys come in
  * non-decreasing order with no NULL, as the lines of a file sorted by the key do (TPC-H lineitem
  * by `l_orderkey`): a key equal to the last group's key is that group, and a greater one is a new
  * group, so no key is hashed. The first key below the last group's key, or the first NULL, inserts
  * the groups so far into slots and groups the rest by hashing.
  *
  * With `zeroEmpty` (`lean_agg_results`), a slot holds its group id + 1, so a new slot array is
  * already all empty as allocated and needs no fill; otherwise it holds the group id, and -1 marks
  * an empty slot.
  */
final class LongGroupTable(
    keyType: PType,
    expectedGroups: Int = 0,
    sortedRuns: Boolean = false,
    zeroEmpty: Boolean = false
) extends GroupIndex:
  import LongGroupTable.*

  private val store = GroupTable.KeyStore(keyType).asInstanceOf[GroupTable.LongStore]
  // Whether every key so far came in non-decreasing order with no NULL, with no slots in use
  private var sorted = sortedRuns
  // The group id + base of each slot, or base - 1 when the slot is empty
  private val base =
    if zeroEmpty then
      1
    else
      0

  private var cap   = 0
  private var slots = Array.emptyIntArray
  private var mask  = 0
  if !sorted then
    cap = GroupTable.slotCapacity(expectedGroups)
    slots = emptySlots(cap)
    mask = cap - 1

  // The key of each group in id order, and whether it is the NULL group
  private var values    = new Array[Long](math.max(16, math.min(expectedGroups, 1 << 20)))
  private var count     = 0
  private var nullGroup = -1

  def numGroups: Int = count

  /**
    * Whether the keys so far were grouped as runs of non-decreasing keys, with no slots (for tests)
    */
  private[exec] def isSorted: Boolean = sorted

  def groupIds(keys: Array[Vec], n: Int): Array[Int] =
    val v     = store.normalize(keys(0)).asInstanceOf[LongVec]
    val x     = v.values
    val ns    = v.nulls
    val ids   = new Array[Int](n)
    var start = 0
    if sorted then
      start = runIds(x, ns, n, ids)
      if start == n then
        return ids
      // A key below the last group's, or a NULL: group the rest by hashing
      sorted = false
      cap = GroupTable.slotCapacity(math.max(expectedGroups, count))
      rehash()
    if ns == null then
      var i = start
      while i < n do
        ids(i) = idOf(x(i))
        i += 1
    else
      var i = start
      while i < n do
        ids(i) =
          if ns(i) then
            nullId()
          else
            idOf(x(i))
        i += 1
    ids

  /**
    * Group ids of the rows from the start while their keys are non-NULL and non-decreasing from the
    * last group's key: the number of rows given ids
    */
  private def runIds(x: Array[Long], ns: Array[Boolean], n: Int, ids: Array[Int]): Int =
    // Rows before the first NULL
    var end = n
    if ns != null then
      var j = 0
      while j < end do
        if ns(j) then
          end = j
        j += 1
    if end == 0 then
      return 0
    var g = count - 1
    if g < 0 then
      g = add(x(0))
    var last = values(g)
    var i    = 0
    while i < end do
      val k = x(i)
      if k > last then
        g = add(k)
        last = k
      else if k != last then
        return i
      ids(i) = g
      i += 1
    end

  private def nullId(): Int =
    if nullGroup < 0 then
      nullGroup = add(0L)
    nullGroup

  /** The group of key k, added as a new group when it is not in the table */
  private def idOf(k: Long): Int =
    var idx = hash(k) & mask
    while true do
      val g = slots(idx) - base
      if g < 0 then
        val id = add(k)
        slots(idx) = id + base
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
    rehash()

  /** New slots of capacity `cap`, with every non-NULL group inserted in id order */
  private def rehash(): Unit =
    mask = cap - 1
    slots = emptySlots(cap)
    var g = 0
    while g < count do
      if g != nullGroup then
        val k   = values(g)
        var idx = hash(k) & mask
        while slots(idx) >= base do
          idx = (idx + 1) & mask
        slots(idx) = g + base
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

  /** Slots of capacity cap, all empty */
  private def emptySlots(cap: Int): Array[Int] =
    val a = new Array[Int](cap)
    if base == 0 then
      java.util.Arrays.fill(a, -1)
    a

end LongGroupTable

object LongGroupTable:
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

end LongGroupTable
