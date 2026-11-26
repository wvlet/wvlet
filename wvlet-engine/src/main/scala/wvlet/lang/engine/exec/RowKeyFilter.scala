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
import wvlet.uni.cloud.parquet.ParquetFilter

import java.util.concurrent.atomic.AtomicLong

/**
  * A per-row test of probe keys against the non-NULL integer keys of a hash join's build
  * (`row_join_filters`): true for every build key, so a probe row it rejects has no match. Probe
  * scans apply it to each batch ([[ScanOp.narrow]]) and drop the rejected rows (and NULL keys)
  * before any operator above sees them.
  *
  *   - `Bitmap`: one bit per value of [lo, hi], exact. Used when the range is dense enough.
  *   - `Bloom`: a register-blocked Bloom filter (one 64-bit word per key, 4 bits set) within [lo,
  *     hi], with about 1% false positives at 16 bits per key.
  *   - `Range`: only [lo, hi], for builds with more keys than [[RowKeyFilter.MaxKeys]].
  *
  * A Bloom filter over at most [[RowKeyFilter.MaxSortedKeys]] build rows also keeps their keys
  * sorted (`sorted`), so that [[mayIntersect]] can tell exactly whether a Parquet row group's [min,
  * max] holds a build key ([[prunedSet]], `key_set_row_groups`).
  *
  * A filter that keeps most of the rows it checks pauses: after [[RowKeyFilter.SampleRows]] rows
  * that pass at least `threshold` of the time, the next batches skip it, twice as many after each
  * pause in a row, then it samples again (DuckDB's selectivity-optional filters). The counters are
  * shared by every scan copy and updated without locks: they steer the heuristic only, never which
  * rows are correct to drop.
  */
final class RowKeyFilter private (
    val kind: RowKeyFilter.Kind,
    lo: Long,
    hi: Long,
    bits: Array[Long],
    shift: Int,
    keys: Int,
    sorted: Array[Long] = null
):
  import RowKeyFilter.*

  /** The fraction of passing rows at which the filter pauses */
  val threshold: Double =
    kind match
      case Kind.Range =>
        0.9
      case _ =>
        0.5

  private val batches = AtomicLong(0L)
  private val seen    = AtomicLong(0L)
  private val passed  = AtomicLong(0L)
  private val dropped = AtomicLong(0L)
  @volatile
  private var until = 0L

  @volatile
  private var pause = 0L

  /** Whether the value x may be a build key */
  def mayContain(x: Long): Boolean =
    kind match
      case Kind.Bitmap =>
        inBitmap(x)
      case Kind.Bloom =>
        inBloom(x)
      case Kind.Range =>
        x >= lo && x <= hi

  private inline def inBitmap(x: Long): Boolean =
    x >= lo && x <= hi && {
      val d = x - lo
      (bits((d >>> 6).toInt) & (1L << d)) != 0
    }

  private inline def inBloom(x: Long): Boolean =
    x >= lo && x <= hi && {
      val h = mix(x)
      val m = maskOf(h)
      (bits((h >>> shift).toInt) & m) == m
    }

  /**
    * The number of values in [a, b] the filter accepts: exact for a bitmap or a range, and for a
    * Bloom filter its build keys scaled by how much of [lo, hi] lies in [a, b], plus about 1% of
    * the other values there (its false positives)
    */
  def acceptedIn(a: Long, b: Long): Double =
    val from = math.max(a, lo)
    val to   = math.min(b, hi)
    if from > to then
      0.0
    else
      kind match
        case Kind.Bitmap =>
          // The set bits of positions [from - lo, to - lo]: whole words from the prefix counts
          val d0       = from - lo
          val d1       = to - lo
          val w0       = (d0 >>> 6).toInt
          val w1       = (d1 >>> 6).toInt
          val lowMask  = -1L << d0
          val highMask = -1L >>> (63 - (d1 & 63))
          if w0 == w1 then
            java.lang.Long.bitCount(bits(w0) & lowMask & highMask).toDouble
          else
            val inner = bitsBefore(w1) - bitsBefore(w0 + 1)
            (
              java.lang.Long.bitCount(bits(w0) & lowMask) + inner +
                java.lang.Long.bitCount(bits(w1) & highMask)
            ).toDouble
        case Kind.Bloom =>
          // The build keys in the overlap, plus the false positives among the other values
          val width = to.toDouble - from.toDouble + 1.0
          keys.toDouble * width / (hi.toDouble - lo.toDouble + 1.0) + BloomFalsePositives * width
        case Kind.Range =>
          to.toDouble - from.toDouble + 1.0

  // For a bitmap, the set bits in the words before each word index (one more entry than words),
  // so that acceptedIn counts any range in constant time
  private lazy val prefixBits: Array[Long] =
    val p = new Array[Long](bits.length + 1)
    var w = 0
    while w < bits.length do
      p(w + 1) = p(w) + java.lang.Long.bitCount(bits(w))
      w += 1
    p

  private inline def bitsBefore(w: Int): Long = prefixBits(w)

  /**
    * Whether some value in [a, b] may be a build key: exact for a bitmap and for a Bloom filter
    * keeping its sorted keys, otherwise whether [a, b] overlaps [lo, hi]
    */
  def mayIntersect(a: Long, b: Long): Boolean =
    if a > hi || b < lo then
      false
    else
      kind match
        case Kind.Bitmap =>
          acceptedIn(a, b) > 0.0
        case Kind.Bloom if sorted != null =>
          // The first key at or above a, if any, must be at most b
          var l = 0
          var r = sorted.length
          while l < r do
            val m = (l + r) >>> 1
            if sorted(m) < a then
              l = m + 1
            else
              r = m
          l < sorted.length && sorted(l) <= b
        case _ =>
          true

  /**
    * Whether [[mayIntersect]] skips more row groups than the [lo, hi] range does: a bitmap, or a
    * Bloom filter keeping its sorted keys
    */
  def prunesBeyondRange: Boolean = kind == Kind.Bitmap || (kind == Kind.Bloom && sorted != null)

  /**
    * The filter as a uni set ([[ParquetFilter.In]]), so that a Parquet scan tests it while decoding
    * and decodes the other columns only for the rows it accepts. It skips the row groups whose
    * statistics lie outside [lo, hi]
    */
  lazy val integerSet: ParquetFilter.IntegerSet = new KeySet(pruneByKeys = false)

  /**
    * [[integerSet]] that also skips the row groups whose [min, max] holds no build key
    * ([[mayIntersect]], `key_set_row_groups`): a few build keys on a probe column clustered in the
    * file (TPC-DS store_sales by ticket number) leave most row groups unread
    */
  lazy val prunedSet: ParquetFilter.IntegerSet = new KeySet(pruneByKeys = true)

  private final class KeySet(pruneByKeys: Boolean) extends ParquetFilter.IntegerSet:
    def contains(v: Long): Boolean                       = mayContain(v)
    def min: Long                                        = lo
    def max: Long                                        = hi
    override def mayIntersect(a: Long, b: Long): Boolean =
      if pruneByKeys then
        RowKeyFilter.this.mayIntersect(a, b)
      else
        a <= hi && b >= lo

    override def toString: String = RowKeyFilter.this.toString

  /** Probe rows the filter has dropped so far, over every scan applying it */
  def droppedRows: Long = dropped.get()

  /**
    * Whether to apply the filter to the next batch. Counts the batch, so call it once per batch
    */
  def active(): Boolean = batches.incrementAndGet() > until

  /** Report that the filter kept `kept` of `checked` rows */
  def record(checked: Int, kept: Int): Unit =
    if kept < checked then
      dropped.addAndGet(checked - kept)
    val s = seen.addAndGet(checked)
    val p = passed.addAndGet(kept)
    if s >= SampleRows then
      seen.set(0L)
      passed.set(0L)
      if p >= threshold * s then
        pause = math.min(math.max(MinPause, pause * 2), MaxPause)
        until = batches.get() + pause
      else
        pause = 0L

  /**
    * Write to `out` the positions of `sel(0 until n)` (rows `0 until n` when `sel` is null) whose
    * key in `keys` passes, and return their count. `out` may be `sel`. Keys that are not integer
    * vectors pass every row
    */
  def select(keys: Vec, sel: Array[Int], n: Int, out: Array[Int]): Int =
    keys match
      case v: IntVec =>
        val a = v.values
        kind match
          case Kind.Bitmap =>
            loop(v.nulls, sel, n, out, i => inBitmap(a(i).toLong))
          case Kind.Bloom =>
            loop(v.nulls, sel, n, out, i => inBloom(a(i).toLong))
          case Kind.Range =>
            loop(v.nulls, sel, n, out, i => a(i) >= lo && a(i) <= hi)
      case v: LongVec =>
        val a = v.values
        kind match
          case Kind.Bitmap =>
            loop(v.nulls, sel, n, out, i => inBitmap(a(i)))
          case Kind.Bloom =>
            loop(v.nulls, sel, n, out, i => inBloom(a(i)))
          case Kind.Range =>
            loop(v.nulls, sel, n, out, i => a(i) >= lo && a(i) <= hi)
      case _ =>
        var j = 0
        while j < n do
          out(j) =
            if sel == null then
              j
            else
              sel(j)
          j += 1
        n

  private inline def loop(
      nulls: Array[Boolean],
      sel: Array[Int],
      n: Int,
      out: Array[Int],
      inline pass: Int => Boolean
  ): Int =
    var k = 0
    var j = 0
    if sel == null && nulls == null then
      while j < n do
        if pass(j) then
          out(k) = j
          k += 1
        j += 1
    else
      while j < n do
        val i =
          if sel == null then
            j
          else
            sel(j)
        if (nulls == null || !nulls(i)) && pass(i) then
          out(k) = i
          k += 1
        j += 1
    k

  override def toString: String = s"${kind}[${lo}, ${hi}]"

end RowKeyFilter

object RowKeyFilter:
  enum Kind:
    case Bitmap,
      Bloom,
      Range

  /** Above how many build rows only the range is tested */
  val MaxKeys: Int = 1 << 22

  /** Bloom filter bits per build row (at least; the word count is a power of two) */
  val BloomBits: Int = 16

  /** About the fraction of other values a Bloom filter with [[BloomBits]] per key accepts */
  val BloomFalsePositives: Double = 0.01

  /** Up to how many bits per build row a bitmap may use instead of a Bloom filter */
  val BitmapBits: Int = 32

  /**
    * Up to how many build rows a Bloom filter keeps its keys sorted, for skipping row groups
    * ([[RowKeyFilter.mayIntersect]]). More keys hit nearly every row group of a probe column they
    * spread over
    */
  val MaxSortedKeys: Int = 1 << 12

  /** Ranges up to this many values always use a bitmap (8 KB) */
  val SmallBitmap: Long = 1L << 16

  /** Rows a filter checks before deciding whether to pause */
  val SampleRows: Long = 1L << 16

  /** Batches a filter skips after its first pause, and at most */
  val MinPause: Long = 4L
  val MaxPause: Long = 256L

  private[exec] inline def mix(x: Long): Long =
    var h = x
    h ^= h >>> 33
    h *= 0xff51afd7ed558ccdL
    h ^= h >>> 33
    h *= 0xc4ceb9fe1a85ec53L
    h ^ (h >>> 33)

  private[exec] inline def maskOf(h: Long): Long =
    (1L << h) | (1L << (h >>> 6)) | (1L << (h >>> 12)) | (1L << (h >>> 18))

  /**
    * The filter for the build keys at rows `rows(0 until n)` (all non-NULL) of `keys`, whose range
    * is [lo, hi], or None for keys that are not integer vectors
    */
  def of(keys: Vec, rows: Array[Int], n: Int, lo: Long, hi: Long): Option[RowKeyFilter] =
    val ints: Array[Int] =
      keys match
        case v: IntVec =>
          v.values
        case _ =>
          null
    val longs: Array[Long] =
      keys match
        case v: LongVec =>
          v.values
        case _ =>
          null
    if ints == null && longs == null then
      return None
    inline def key(j: Int): Long =
      if ints != null then
        ints(rows(j)).toLong
      else
        longs(rows(j))
    // hi - lo + 1 overflows only for ranges far beyond any bitmap
    val span = hi - lo + 1
    if n > MaxKeys then
      Some(RowKeyFilter(Kind.Range, lo, hi, null, 0, n))
    else if span > 0 && (span <= SmallBitmap || span <= BitmapBits.toLong * n) then
      val bits = new Array[Long](((span + 63) >>> 6).toInt)
      var j    = 0
      while j < n do
        val d = key(j) - lo
        bits((d >>> 6).toInt) |= 1L << d
        j += 1
      Some(RowKeyFilter(Kind.Bitmap, lo, hi, bits, 0, n))
    else
      // At least two words, so that the word index shift stays below 64
      val words = Integer.highestOneBit(math.max(2, (n.toLong * BloomBits / 64).toInt) * 2 - 1)
      val bits  = new Array[Long](words)
      val shift = 64 - Integer.numberOfTrailingZeros(words)
      var j     = 0
      while j < n do
        val h = mix(key(j))
        bits((h >>> shift).toInt) |= maskOf(h)
        j += 1
      val sorted =
        if n <= MaxSortedKeys then
          val a = new Array[Long](n)
          var i = 0
          while i < n do
            a(i) = key(i)
            i += 1
          java.util.Arrays.sort(a)
          a
        else
          null
      Some(RowKeyFilter(Kind.Bloom, lo, hi, bits, shift, n, sorted))
    end if

  end of

end RowKeyFilter
