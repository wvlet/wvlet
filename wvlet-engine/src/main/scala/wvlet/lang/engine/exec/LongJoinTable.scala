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
  * A join's hash table over one Int64 key, by open addressing over entries that hold a key and its
  * first build row side by side, so a probe reads one cache line instead of a slot, then the stored
  * key, then the group's first row (as DuckDB's salted entries and DataFusion's JoinHashMap).
  *
  * When the build keys span less than 2^32 values, an entry is one Long, `(key - min) << 32 | row`,
  * so the table is half as large; otherwise an entry is two Longs, the key then the row. Empty
  * entries are -1 (no row is -1).
  *
  * The entries are split into one region per hash partition, picked by the top bits of the key
  * hash; the low bits pick the entry inside it. Partitions are built in parallel, each writing its
  * own region, and a probe goes straight to a row's region and entry, so it reads probe keys in row
  * order with no partition split or gather. Built once, then only read.
  */
final class LongJoinTable private (
    entries: Array[Long],
    partBits: Int,
    regionBits: Int,
    // The smallest key, and the largest key - min; narrow entries when range < 2^32
    min: Long,
    range: Long
):
  import LongJoinTable.*

  private val regionMask = (1 << regionBits) - 1
  private val narrow     = range < (1L << 32)

  /** The first slot of key hash h */
  private inline def slotOf(h: Int): Int =
    (((h.toLong & 0xffffffffL) >>> (32 - partBits)).toInt << regionBits) | (h & regionMask)

  /** The next slot after s, wrapping inside s's region */
  private inline def nextSlot(s: Int): Int = (s & ~regionMask) | ((s + 1) & regionMask)

  /**
    * The first build row of each of the first n probe keys, or -1 when no build row has the key or
    * the key is NULL (`nulls`, which may be null)
    */
  def firstRows(x: Array[Long], nulls: Array[Boolean], n: Int, out: Array[Int]): Unit =
    probe(n, nulls, out)(i => x(i))

  /** [[firstRows]] for Int32 probe keys, compared as Int64 */
  def firstRows(x: Array[Int], nulls: Array[Boolean], n: Int, out: Array[Int]): Unit =
    probe(n, nulls, out)(i => x(i).toLong)

  private inline def probe(n: Int, nulls: Array[Boolean], out: Array[Int])(
      inline keyAt: Int => Long
  ): Unit =
    val e = entries
    // Slots first, in a pass of their own: the entry loads below then do not wait on hashing, and
    // the cache misses of neighboring rows overlap
    var i = 0
    while i < n do
      out(i) = slotOf(hash(keyAt(i)))
      i += 1
    i = 0
    if narrow then
      while i < n do
        val d = keyAt(i) - min
        // Keys outside the build range (d < 0 also when the subtraction overflowed) match nothing
        var row = -1
        if d >= 0 && d <= range && (nulls == null || !nulls(i)) then
          var s         = out(i)
          var searching = true
          while searching do
            val x = e(s)
            if x == Empty then
              searching = false
            else if (x >>> 32) == d then
              row = x.toInt
              searching = false
            else
              s = nextSlot(s)
        out(i) = row
        i += 1
    else
      while i < n do
        val key       = keyAt(i)
        var s         = out(i)
        var row       = -1
        var searching = true
        while searching do
          val r = e(2 * s + 1).toInt
          if r < 0 then
            searching = false
          else if e(2 * s) == key then
            row = r
            searching = false
          else
            s = nextSlot(s)
        out(i) =
          if nulls != null && nulls(i) then
            -1
          else
            row
        i += 1
    end if

  end probe

  /** Insert the build rows in reverse, so chains enumerate them in input order */
  private def insertAll(key: Array[Long], rows: Array[Int], chain: Array[Int]): Boolean =
    val e   = entries
    var dup = false
    var j   = rows.length - 1
    while j >= 0 do
      val row  = rows(j)
      val k    = key(row)
      var s    = slotOf(hash(k))
      var done = false
      if narrow then
        val d = k - min
        while !done do
          val x = e(s)
          if x == Empty then
            e(s) = (d << 32) | row.toLong
            done = true
          else if (x >>> 32) == d then
            chain(row) = x.toInt
            e(s) = (d << 32) | row.toLong
            dup = true
            done = true
          else
            s = nextSlot(s)
      else
        while !done do
          val r = e(2 * s + 1).toInt
          if r < 0 then
            e(2 * s) = k
            e(2 * s + 1) = row
            done = true
          else if e(2 * s) == k then
            chain(row) = r
            e(2 * s + 1) = row
            dup = true
            done = true
          else
            s = nextSlot(s)
      end if
      j -= 1
    end while
    !dup

  end insertAll

end LongJoinTable

object LongJoinTable:
  private inline val Empty = -1L

  /**
    * A table takes at most this many slots per build row. Balanced partitions take at most 4, so
    * more means a frequent key crowds one partition: GroupTables, sized by distinct keys, fit such
    * builds better
    */
  inline val MaxSlotsPerRow = 8

  /** The most slots of a table, so that the indexes of two-Long entries fit in an Int */
  inline val MaxSlots = 1L << 28

  /** The key hash; the top bits pick the partition, the low bits the slot */
  inline def hash(x: Long): Int = GroupTable.fmix(GroupTable.hashLong(x))

  /**
    * A table over the build rows `rowsOf(p)` of each of the 2^partBits partitions (by the top bits
    * of [[hash]]), whose keys are `key(row)`. `chain(row)` is set to the next row with the same
    * key, in input order, and must start at -1. Returns the table and whether no two rows share a
    * key, or a null table, leaving `chain` untouched, when it would take more than
    * [[MaxSlotsPerRow]] slots per row (partitions skewed by a frequent key) or [[MaxSlots]] slots
    */
  def build(
      key: Array[Long],
      rowsOf: Array[Array[Int]],
      partBits: Int,
      chain: Array[Int],
      runner: TaskRunner
  ): (LongJoinTable, Boolean) =
    val parts   = rowsOf.length
    var maxRows = 0
    var rows    = 0L
    var lo      = Long.MaxValue
    var hi      = Long.MinValue
    var p       = 0
    while p < parts do
      maxRows = math.max(maxRows, rowsOf(p).length)
      rows += rowsOf(p).length
      var j = 0
      while j < rowsOf(p).length do
        val k = key(rowsOf(p)(j))
        if k < lo then
          lo = k
        if k > hi then
          hi = k
        j += 1
      p += 1
    // A negative range overflowed a Long: wide entries
    val range =
      if maxRows == 0 then
        0L
      else if hi - lo < 0 then
        Long.MaxValue
      else
        hi - lo
    // Regions at most half full
    val regionBits = math.max(
      3,
      64 - java.lang.Long.numberOfLeadingZeros(math.max(1L, maxRows * 2L - 1))
    )
    val slots = parts.toLong << regionBits
    if slots > MaxSlots || slots > math.max(1024L, rows * MaxSlotsPerRow) then
      return (null, false)
    val width =
      if range < (1L << 32) then
        1
      else
        2
    val entries = new Array[Long](width * slots.toInt)
    java.util.Arrays.fill(entries, Empty)
    val table  = LongJoinTable(entries, partBits, regionBits, lo, range)
    val unique = new Array[Boolean](parts)
    if parts == 1 then
      unique(0) = table.insertAll(key, rowsOf(0), chain)
    else
      (0 until parts)
        .map(p => runner.submit(() => unique(p) = table.insertAll(key, rowsOf(p), chain)))
        .foreach(_.get())
    (table, !unique.contains(false))

  end build

end LongJoinTable
