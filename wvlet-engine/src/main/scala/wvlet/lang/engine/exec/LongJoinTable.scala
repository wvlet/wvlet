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
  * When the build keys span fewer values than fit beside a row number, an entry is one Long,
  * `(key - min) << rowBits | row`, so the table is half as large; otherwise an entry is two Longs,
  * the key then the row. With `fitRowBits` (`narrow_join_entries`), `rowBits` is the fewest bits
  * that hold every build row, so packed multi-column keys of up to 64 - rowBits bits (TPC-DS q78's
  * year, item and customer, about 39 bits) still take one Long; otherwise it is 32. Empty entries
  * are -1 (no row is all ones in its bits).
  *
  * The entries are split into one region per hash partition, picked by the top bits of the key
  * hash; the low bits pick the entry inside it. Partitions are built in parallel, each writing its
  * own region, and a probe goes straight to a row's region and entry, so it reads probe keys in row
  * order with no partition split or gather. Built once, then only read.
  *
  * With `missFilter` (`probe_miss_filter`), the table also keeps a register-blocked Bloom filter of
  * its keys (one 64-bit word per key, 4 bits set, about 8 bits per key), split into the same
  * partitions, so a probe reads a word of a filter that stays in cache before the entries, which
  * take 16 to 64 bytes per key. Probes of joins that keep their unmatched rows (anti joins, the
  * unmatched-only LEFT joins of TPC-DS q78) mostly miss, and the entries of a large build are out
  * of cache.
  */
final class LongJoinTable private (
    entries: Array[Long],
    partBits: Int,
    regionBits: Int,
    // The smallest key, and the largest key - min; narrow entries when range < 2^(64 - rowBits)
    min: Long,
    range: Long,
    // The low bits of a narrow entry that hold its row
    rowBits: Int,
    // The Bloom filter words of each partition, 2^bloomBits of them, or null
    bloom: Array[Long],
    bloomBits: Int
):
  import LongJoinTable.*

  private val regionMask = (1 << regionBits) - 1
  private val narrow     = fitsNarrow(range, rowBits)
  private val rowMask    = (1L << rowBits) - 1

  /** The first slot of key hash h */
  private inline def slotOf(h: Int): Int =
    (((h.toLong & 0xffffffffL) >>> (32 - partBits)).toInt << regionBits) | (h & regionMask)

  /** The Bloom word of key hash h and key mix m, in h's partition */
  private inline def wordOf(h: Int, m: Long): Int =
    (((h.toLong & 0xffffffffL) >>> (32 - partBits)).toInt << bloomBits) |
      (m >>> (64 - bloomBits)).toInt

  /** Whether an entry is one Long (for tests) */
  def hasNarrowEntries: Boolean = narrow

  /** Whether the table keeps a Bloom filter of its keys (for tests) */
  def hasMissFilter: Boolean = bloom != null

  /** The bytes of the entries and the Bloom filter */
  def sizeBytes: Long =
    8L * entries.length + (
      if bloom != null then
        8L * bloom.length
      else
        0L
    )

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
    // the cache misses of neighboring rows overlap. A key the Bloom filter rejects gets slot -1
    var i = 0
    if bloom == null then
      while i < n do
        out(i) = slotOf(hash(keyAt(i)))
        i += 1
    else
      val f = bloom
      while i < n do
        val k = keyAt(i)
        val h = hash(k)
        val m = RowKeyFilter.mix(k)
        val b = RowKeyFilter.maskOf(m)
        out(i) =
          if (f(wordOf(h, m)) & b) == b then
            slotOf(h)
          else
            -1
        i += 1
    i = 0
    if narrow then
      while i < n do
        val d = keyAt(i) - min
        // Keys outside the build range (d < 0 also when the subtraction overflowed) match nothing
        var row = -1
        if d >= 0 && d <= range && out(i) >= 0 && (nulls == null || !nulls(i)) then
          var s         = out(i)
          var searching = true
          while searching do
            val x = e(s)
            if x == Empty then
              searching = false
            else if (x >>> rowBits) == d then
              row = (x & rowMask).toInt
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
        var searching = s >= 0
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

  /** Add the keys of build rows `rows` to the Bloom filter */
  private def addToFilter(key: Array[Long], rows: Array[Int]): Unit =
    val f = bloom
    var j = 0
    while j < rows.length do
      val k = key(rows(j))
      val m = RowKeyFilter.mix(k)
      val w = wordOf(hash(k), m)
      f(w) = f(w) | RowKeyFilter.maskOf(m)
      j += 1

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
            e(s) = (d << rowBits) | row.toLong
            done = true
          else if (x >>> rowBits) == d then
            chain(row) = (x & rowMask).toInt
            e(s) = (d << rowBits) | row.toLong
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

  /**
    * The smallest table, in bytes of entries, that keeps a Bloom filter with `missFilter`: smaller
    * ones mostly stay in cache, so probing them directly is as fast
    */
  inline val MinMissFilterBytes = 16L << 20

  /** Whether keys spanning `range` fit beside `rowBits` row bits in one Long */
  private def fitsNarrow(range: Long, rowBits: Int): Boolean =
    rowBits >= 1 && rowBits <= 32 && range < (1L << (64 - rowBits))

  /**
    * The row bits of a narrow entry over `size` build rows: the fewest that hold rows 0 until size
    * without all ones (the empty entry), or 32 without `fitRowBits`
    */
  private[exec] def rowBitsOf(size: Int, fitRowBits: Boolean): Int =
    if fitRowBits then
      64 - java.lang.Long.numberOfLeadingZeros(size.toLong)
    else
      32

  /** The key hash; the top bits pick the partition, the low bits the slot */
  inline def hash(x: Long): Int = GroupTable.fmix(GroupTable.hashLong(x))

  /**
    * A table over the build rows `rowsOf(p)` of each of the 2^partBits partitions (by the top bits
    * of [[hash]]), whose keys are `key(row)`. `chain(row)` is set to the next row with the same
    * key, in input order, and must start at -1. Returns the table and whether no two rows share a
    * key, or a null table, leaving `chain` untouched, when it would take more than
    * [[MaxSlotsPerRow]] slots per row (partitions skewed by a frequent key) or [[MaxSlots]] slots.
    * With `missFilter`, a table of at least `minFilterBytes` of entries also keeps a Bloom filter
    * of its keys. With `fitRowBits` (`narrow_join_entries`), narrow entries give the row only the
    * bits the build rows need
    */
  def build(
      key: Array[Long],
      rowsOf: Array[Array[Int]],
      partBits: Int,
      chain: Array[Int],
      runner: TaskRunner,
      missFilter: Boolean = false,
      minFilterBytes: Long = MinMissFilterBytes,
      fitRowBits: Boolean = true,
      parallelSteps: Boolean = false
  ): (LongJoinTable, Boolean) =
    val parts = rowsOf.length
    // With `parallelSteps` (`parallel_join_keys`), each partition's task takes its key range and
    // fills its own region's empty entries; otherwise the calling thread does both
    val inParts               = parallelSteps && parts > 1
    val los                   = Array.fill(parts)(Long.MaxValue)
    val his                   = Array.fill(parts)(Long.MinValue)
    def rangeOf(p: Int): Unit =
      var lo = Long.MaxValue
      var hi = Long.MinValue
      val r  = rowsOf(p)
      var j  = 0
      while j < r.length do
        val k = key(r(j))
        if k < lo then
          lo = k
        if k > hi then
          hi = k
        j += 1
      los(p) = lo
      his(p) = hi
    JoinBuildSteps.runAll(
      parts,
      if inParts then
        runner
      else
        TaskRunner.Sequential
    )(rangeOf)
    val maxRows = rowsOf.iterator.map(_.length).max
    val rows    = rowsOf.iterator.map(_.length.toLong).sum
    val lo      = los.min
    val hi      = his.max
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
    val rowBits = rowBitsOf(math.max(1, key.length), fitRowBits)
    val width   =
      if fitsNarrow(range, rowBits) then
        1
      else
        2
    val entries = new Array[Long](width * slots.toInt)
    if !inParts then
      java.util.Arrays.fill(entries, Empty)
    // About 8 bits (an eighth of a word) per key of the largest partition, rounded up: 1.75x faster
    // probes of 2.9M keys that miss 90% of the time than 16 or 4 bits
    val bloomBits =
      if missFilter && width * slots * 8L >= minFilterBytes then
        64 - java.lang.Long.numberOfLeadingZeros(math.max(1L, (maxRows + 7L) / 8 - 1))
      else
        0
    val bloom =
      if bloomBits > 0 then
        new Array[Long](parts << bloomBits)
      else
        null
    val table  = LongJoinTable(entries, partBits, regionBits, lo, range, rowBits, bloom, bloomBits)
    val unique = new Array[Boolean](parts)
    // Partitions own disjoint entry regions and Bloom words
    def insertPart(p: Int): Unit =
      if inParts then
        val region = width << regionBits
        java.util.Arrays.fill(entries, p * region, (p + 1) * region, Empty)
      unique(p) = table.insertAll(key, rowsOf(p), chain)
      if bloom != null then
        table.addToFilter(key, rowsOf(p))
    if parts == 1 then
      insertPart(0)
    else
      TaskRunner.runAll(runner, parts)(insertPart)
    (table, !unique.contains(false))

  end build

end LongJoinTable
