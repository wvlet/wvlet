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
package wvlet.lang.engine.vector

import java.math.BigDecimal as JBigDecimal

/**
  * An immutable column of values of one physical type. `nulls` is null when no value is null;
  * otherwise `nulls(i)` is true for NULL rows (the value slot of a NULL row is unspecified).
  *
  * `get(i)` returns the engine value of row i (boxed): Boolean, Int (int32, date), Long (int64,
  * timestamp), Double, java.math.BigDecimal (decimal), String, or any object for AnyT, and null for
  * NULL.
  */
sealed abstract class Vec:
  def ptype: PType
  def size: Int
  def nulls: Array[Boolean]

  final def isNull(i: Int): Boolean = nulls != null && nulls(i)
  final def hasNulls: Boolean       = nulls != null

  def get(i: Int): Any

  /** Rows at the given positions, in order */
  def gather(idx: Array[Int], n: Int): Vec

  /** Rows whose mask is true */
  def filter(mask: Array[Boolean], count: Int): Vec =
    val idx = new Array[Int](count)
    var j   = 0
    var i   = 0
    while i < size do
      if mask(i) then
        idx(j) = i
        j += 1
      i += 1
    gather(idx, count)

  def slice(from: Int, until: Int): Vec =
    val n   = math.max(0, until - from)
    val idx = new Array[Int](n)
    var i   = 0
    while i < n do
      idx(i) = from + i
      i += 1
    gather(idx, n)

  protected def gatherNulls(idx: Array[Int], n: Int): Array[Boolean] =
    if nulls == null then
      null
    else
      val out = new Array[Boolean](n)
      var any = false
      var i   = 0
      while i < n do
        val k = idx(i)
        if k < 0 || nulls(k) then
          out(i) = true
          any = true
        i += 1
      if any then
        out
      else
        null

end Vec

object Vec:
  // Element-wise maps over primitive arrays. Array.map and Array.fill go through generic array
  // updates that box every element; these inline into plain loops

  inline def mapIntToLong(a: Array[Int])(inline f: Int => Long): Array[Long] =
    val out = new Array[Long](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapInt(a: Array[Int])(inline f: Int => Int): Array[Int] =
    val out = new Array[Int](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapLong(a: Array[Long])(inline f: Long => Long): Array[Long] =
    val out = new Array[Long](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  inline def mapFloatToDouble(a: Array[Float]): Array[Double] =
    val out = new Array[Double](a.length)
    var i   = 0
    while i < a.length do
      out(i) = a(i).toDouble
      i += 1
    out

  inline def mapDouble(a: Array[Double])(inline f: Double => Double): Array[Double] =
    val out = new Array[Double](a.length)
    var i   = 0
    while i < a.length do
      out(i) = f(a(i))
      i += 1
    out

  def filledInts(n: Int, v: Int): Array[Int] =
    val a = new Array[Int](n)
    java.util.Arrays.fill(a, v)
    a

  def allTrue(n: Int): Array[Boolean] =
    val a = new Array[Boolean](n)
    java.util.Arrays.fill(a, true)
    a

  /**
    * Nulls of gathered rows: positions may be -1 to produce NULL (outer join padding). Bind the
    * result to a local before passing it to a constructor: a loop inlined into constructor
    * arguments runs with a non-empty operand stack, which HotSpot cannot enter by on-stack
    * replacement, so after a deoptimization a long gather stays in the interpreter
    */
  private[vector] inline def nullsWithPadding(
      nulls: Array[Boolean],
      idx: Array[Int],
      n: Int
  ): Array[Boolean] =
    var out: Array[Boolean] = null
    var i                   = 0
    while i < n do
      val k = idx(i)
      if k < 0 || (nulls != null && nulls(k)) then
        if out == null then
          out = new Array[Boolean](n)
        out(i) = true
      i += 1
    out

  def nullVec(ptype: PType, n: Int): Vec =
    val nulls = allTrue(n)
    ptype match
      case PType.Bool =>
        BoolVec(new Array[Boolean](n), nulls)
      case PType.Int32 | PType.Date =>
        IntVec(ptype, new Array[Int](n), nulls)
      case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
        LongVec(ptype, new Array[Long](n), nulls)
      case d: PType.Decimal =>
        LongVec(d, new Array[Long](n), nulls)
      case PType.Float64 =>
        DoubleVec(new Array[Double](n), nulls)
      case PType.Str =>
        StrVec(new Array[String](n), nulls)
      case _ =>
        ObjVec(ptype, new Array[AnyRef](n), nulls)

  /** A vector repeating one engine value */
  def constant(ptype: PType, value: Any, n: Int): Vec =
    if value == null then
      nullVec(ptype, n)
    else
      ptype match
        // The value is converted once and filled without boxing
        case PType.Bool =>
          val a = new Array[Boolean](n)
          java.util.Arrays.fill(a, ValueOps.toBoolean(value))
          BoolVec(a, null)
        case PType.Int32 | PType.Date =>
          IntVec(ptype, filledInts(n, ValueOps.toInt(value)), null)
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
          val a = new Array[Long](n)
          java.util.Arrays.fill(a, ValueOps.toLong(value))
          LongVec(ptype, a, null)
        case PType.Float64 =>
          val a = new Array[Double](n)
          java.util.Arrays.fill(a, ValueOps.toDouble(value))
          DoubleVec(a, null)
        case PType.Str =>
          val a = new Array[String](n)
          java.util.Arrays.fill(a.asInstanceOf[Array[AnyRef]], ValueOps.toStr(value))
          StrVec(a, null)
        case d: PType.Decimal =>
          val r = ValueOps.rescale(ValueOps.toBigDecimal(value), d.scale)
          val u = r.unscaledValue()
          // Same storage as DecimalBuilder: Long-backed when the unscaled value fits
          if u.bitLength() < 64 then
            val a = new Array[Long](n)
            java.util.Arrays.fill(a, u.longValue())
            LongVec(d, a, null)
          else
            val a = new Array[AnyRef](n)
            java.util.Arrays.fill(a, r)
            ObjVec(d, a, null)
        case _ =>
          val b = VecBuilder(ptype, n)
          var i = 0
          while i < n do
            b.add(value)
            i += 1
          b.result()

  def fromValues(ptype: PType, values: Seq[Any]): Vec =
    val b = VecBuilder(ptype, values.size)
    values.foreach(b.add)
    b.result()

end Vec

final class BoolVec(val values: Array[Boolean], val nulls: Array[Boolean]) extends Vec:
  def ptype: PType     = PType.Bool
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Boolean](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    BoolVec(out, outNulls)

final class IntVec(val ptype: PType, val values: Array[Int], val nulls: Array[Boolean]) extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    IntVec(ptype, out, outNulls)

/** int64, timestamp (micros), and Long-backed decimal (unscaled) values */
final class LongVec(val ptype: PType, val values: Array[Long], val nulls: Array[Boolean])
    extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      ptype match
        case PType.Decimal(_, s) =>
          JBigDecimal.valueOf(values(i), s)
        case _ =>
          values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Long](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    LongVec(ptype, out, outNulls)

final class DoubleVec(val values: Array[Double], val nulls: Array[Boolean]) extends Vec:
  def ptype: PType     = PType.Float64
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[Double](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    DoubleVec(out, outNulls)

/**
  * Strings. A vector decoded from a Parquet dictionary may also carry the dictionary `codes` of its
  * rows (null without them), each in [0, `dictSize`) or -1 when unknown (a NULL row, a
  * PLAIN-encoded row, or gathered padding). Two rows of one vector with the same code >= 0 hold
  * equal strings, so grouping can look up each code once ([[wvlet.lang.engine.exec.GroupTable]]).
  * Codes mean nothing across vectors, unless the vectors share a `dict`.
  *
  * A non-null `dict` holds the distinct values the codes stand for (`dict(codes(i)) == values(i)`,
  * `dictSize == dict.length`, -1 only for NULL rows), and vectors with the same `dict` instance
  * share the coding, so a code identifies a value across batches. Hash-join builds encode their
  * dictionary-encoded string columns this way ([[StrVec.encodeParts]], `join_build_string_codes`).
  *
  * A vector built with `entries` and no `values` ([[StrVec.ofEntries]], `lazy_dictionary_strings`)
  * holds only the codes and the strings of its batch's dictionary entries: row i is
  * `entries(codes(i))`, or NULL for code -1. [[str]] reads one row without building the per-row
  * array, which [[values]] builds on first use, so grouping by the codes never builds it.
  *
  * A vector built with `bytes` ([[StrVec.ofBytes]], `plain_string_bytes`) holds its rows as UTF-8
  * bytes ([[StrVec.Bytes]]). Kernels that read them ([[bytesView]]) never build a String;
  * [[values]] decodes them on first use and then drops the bytes, so the vector holds Strings only
  * from then on. A gather before that keeps the source's bytes and composes the row index instead
  * of copying, so a join fanning a dimension's rows out to many output rows doesn't copy their
  * bytes per output row. A view builds its Strings from its source's, so each source row is decoded
  * once and equal rows share one String.
  */
final class StrVec(
    initValues: Array[String],
    val nulls: Array[Boolean],
    initCodes: Array[Int] = null,
    val dictSize: Int = 0,
    val dict: Array[String] = null,
    codeSource: Array[Int] = null,
    codeIndex: Array[Int] = null,
    entries: Array[String] = null,
    rows: Int = -1,
    initBytes: StrVec.Bytes = null
) extends Vec:
  // The rows' bytes until the Strings are built, then null: a vector that built its Strings
  // doesn't keep its source page's bytes (or a view its source's Strings) alive. Written after
  // `built`, so a reader that sees null here also sees `built`
  @volatile
  private var heldBytes: StrVec.Bytes = initBytes

  // Whether the rows came as bytes (a field of its own, so `initBytes` is not kept)
  private val fromBytes: Boolean = initBytes != null

  // The codes, gathered from `codeSource` at `codeIndex` on first use: most gathered vectors (join
  // outputs, filtered scans) never read them. Racing first uses compute equal arrays
  @volatile
  private var gatheredCodes: Array[Int] = initCodes

  // The per-row strings of a vector built from entries, once built. The holder's final field
  // publishes the filled array safely to other threads without a volatile read per access
  private var built: StrVec.Built = null

  /** The dictionary codes of the rows, or null without them */
  def codes: Array[Int] =
    var c = gatheredCodes
    if c == null && codeSource != null then
      c = StrVec.gatherCodes(codeSource, codeIndex, size)
      gatheredCodes = c
    c

  /** Whether the rows carry codes, without gathering them */
  def hasCodes: Boolean = gatheredCodes != null || codeSource != null

  /** Whether the rows are held as codes into `entries`, without a per-row array yet */
  def holdsEntries: Boolean = initValues == null && entries != null

  /** Whether the rows are still held as UTF-8 bytes ([[bytesView]]) */
  def holdsBytes: Boolean = heldBytes != null

  /**
    * The rows' UTF-8 bytes, or null once the Strings are built (or without bytes). Kernels read
    * this once per batch: the vector may drop it while they run
    */
  def bytesView: StrVec.Bytes = heldBytes

  /** The batch's dictionary entries of a vector that [[holdsEntries]] (null otherwise) */
  private[engine] def entryStrings: Array[String] = entries

  /** The string of each row (null for NULL rows), built from the entries on first use */
  def values: Array[String] =
    if initValues != null then
      initValues
    else
      // Read the bytes before `built`: once they are dropped, `built` is visible
      val hb = heldBytes
      var b  = built
      if b == null then
        b =
          if hb == null then
            StrVec.Built(StrVec.decodeEntries(entries, codes, size))
          else if hb.source != null then
            // Share the source's Strings: each source row is decoded once for all its views, as
            // a scan without plain_string_bytes would, and equal rows stay one String instance
            StrVec.Built(StrVec.gatherStrings(hb.source.values, hb.index, size))
          else
            // Views of this vector on other threads may ask at once: decode once
            this.synchronized {
              if built == null then
                built = StrVec.Built(StrVec.decodeBytes(hb.data, hb.offsets, nulls, size))
              built
            }
        built = b
        if hb != null then
          heldBytes = null
      b.values

  /** Row i's string (null for a NULL row), without building [[values]] */
  def str(i: Int): String =
    if initValues != null then
      initValues(i)
    else if built != null then
      built.values(i)
    else if heldBytes != null then
      // Build the Strings once rather than decoding row i on each read: readers of single rows
      // (IN lists, CASE) often read every row, and a later reader would decode them again
      values(i)
    else
      val c = codes(i)
      if c >= 0 then
        entries(c)
      else
        null

  def ptype: PType = PType.Str
  val size: Int    =
    if initValues != null then
      initValues.length
    else
      rows

  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      str(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    val c        = gatheredCodes
    if fromBytes then
      val hb = heldBytes
      val b  = built
      if hb == null || b != null then
        return StrVec(StrVec.gatherStrings(values, idx, n), outNulls)
      // A view of the same source rows, without copying their bytes
      val at =
        if hb.index == null then
          java.util.Arrays.copyOf(idx, n)
        else
          StrVec.gatherCodes(hb.index, idx, n)
      val src =
        if hb.source != null then
          hb.source
        else
          this
      return StrVec(
        null,
        outNulls,
        rows = n,
        initBytes = StrVec.Bytes(hb.data, hb.offsets, at, src)
      )
    if initValues == null then
      // Gather the codes only, keeping the entries
      val src =
        if c != null then
          c
        else
          codeSource
      val at =
        if c != null then
          java.util.Arrays.copyOf(idx, n)
        else
          StrVec.gatherCodes(codeIndex, idx, n)
      return StrVec(null, outNulls, null, dictSize, dict, src, at, entries, n)
    val out = new Array[String](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = initValues(k)
      i += 1
    if c != null then
      StrVec(out, outNulls, null, dictSize, dict, c, java.util.Arrays.copyOf(idx, n))
    else if codeSource != null then
      // Positions in the codes this vector would gather
      StrVec(out, outNulls, null, dictSize, dict, codeSource, StrVec.gatherCodes(codeIndex, idx, n))
    else
      StrVec(out, outNulls)

  end gather

end StrVec

object StrVec:
  /**
    * `v`, the concatenation of `parts`, with codes over its distinct values and their `dict`, so
    * that the vectors gathered from it share one coding. A part carrying Parquet dictionary codes
    * ([[codes]]) hashes each entry once, not each row; a part without them hashes each row when
    * `hashPlain`. `v` itself when a part has no codes (without `hashPlain`) or the values have more
    * than `maxDistinct` distinct values.
    *
    * With `probe` (`string_code_probe`), `v` is given up after the first [[probeRows]] non-null
    * rows when they repeat fewer than [[ProbeMinRepeats]] values: rows drawn at random from at most
    * `maxDistinct` values would repeat about 4x as many, so the column is nearly unique and would
    * fail anyway, after hashing `maxDistinct` rows instead of these few. Values stored in any order
    * repeat more often than at random, except when every value's first row comes before every
    * value's second, and then only the codes are lost
    */
  def encodeParts(
      v: StrVec,
      parts: Seq[Vec],
      maxDistinct: Int,
      hashPlain: Boolean,
      probe: Boolean = false
  ): StrVec =
    val ids   = java.util.HashMap[String, Integer]()
    val dict  = scala.collection.mutable.ArrayBuffer.empty[String]
    val codes = new Array[Int](v.size)
    // The non-null rows after which the repeats are counted, never with `probe` off
    val probeAt =
      if probe then
        probeRows(maxDistinct)
      else
        Int.MaxValue
    var seen = 0
    // Whether the values are nearly unique, once probeAt rows are seen
    inline def unique(): Boolean =
      seen += 1
      seen == probeAt && seen - dict.size < ProbeMinRepeats
    // The code of a value, new values taking the next code; -1 past maxDistinct
    def codeOf(s: String): Int =
      val c = ids.get(s)
      if c != null then
        c.intValue
      else if dict.size >= maxDistinct then
        -1
      else
        ids.put(s, dict.size)
        dict += s
        dict.size - 1
    var off = 0
    val it  = parts.iterator
    while it.hasNext do
      it.next() match
        case p: StrVec if p.hasCodes && p.dictSize > 0 =>
          val m = p.size
          if off + m > codes.length then
            return v
          // The code of each entry of the part's dictionary seen so far
          val local = new Array[Int](p.dictSize)
          java.util.Arrays.fill(local, -1)
          var i = 0
          while i < m do
            val s = p.values(i)
            codes(off + i) =
              if s == null || p.isNull(i) then
                -1
              else
                val x = p.codes(i)
                val c =
                  if x < 0 then
                    codeOf(s)
                  else if local(x) >= 0 then
                    local(x)
                  else
                    val y = codeOf(s)
                    local(x) = y
                    y
                if c < 0 || unique() then
                  return v
                c
            i += 1
          off += m
        case p: StrVec if hashPlain =>
          val m = p.size
          if off + m > codes.length then
            return v
          var i = 0
          while i < m do
            val s = p.values(i)
            codes(off + i) =
              if s == null || p.isNull(i) then
                -1
              else
                val c = codeOf(s)
                if c < 0 || unique() then
                  return v
                c
            i += 1
          off += m
        case _ =>
          return v
    end while
    if off != codes.length then
      v
    else
      StrVec(v.values, v.nulls, codes, dict.size, dict.toArray)

  end encodeParts

  /** At least how many repeats the first [[probeRows]] rows of a column worth encoding hold */
  inline val ProbeMinRepeats = 16

  /**
    * The rows after which [[encodeParts]] counts repeats: k rows drawn at random from d values
    * repeat about k * k / (2 * d) of them, 64 (4x [[ProbeMinRepeats]]) for d = `maxDistinct`
    */
  def probeRows(maxDistinct: Int): Int = math.ceil(math.sqrt(128.0 * maxDistinct)).toInt

  /** The per-row strings of an entries-backed vector, published through a final field */
  final private class Built(val values: Array[String])

  /**
    * `n` rows held as `codes` into the strings of their batch's dictionary `entries` (code -1 for a
    * NULL row), without a per-row array until one is asked for ([[StrVec.values]])
    */
  def ofEntries(entries: Array[String], codes: Array[Int], nulls: Array[Boolean], n: Int): StrVec =
    StrVec(null, nulls, codes, entries.length, entries = entries, rows = n)

  /**
    * `n` rows held as UTF-8 bytes: row i is `bytes` from `offsets(i)` to `offsets(i + 1)`, or NULL
    * where `nulls(i)`. The arrays are shared, not copied
    */
  def ofBytes(bytes: Array[Byte], offsets: Array[Int], nulls: Array[Boolean], n: Int): StrVec =
    StrVec(null, nulls, rows = n, initBytes = Bytes(bytes, offsets, null, null))

  /**
    * The UTF-8 bytes of a vector's rows: row i is source row r = `index(i)` (i without an index, -1
    * for padding), whose bytes run from `offsets(r)` to `offsets(r + 1)`. `source` is the vector
    * that owns `data` for a view (a gathered vector), or null for that vector itself
    */
  final class Bytes(
      val data: Array[Byte],
      val offsets: Array[Int],
      val index: Array[Int],
      val source: StrVec
  ):
    /** The source row of row i, or -1 for padding */
    inline def rowOf(i: Int): Int =
      if index == null then
        i
      else
        index(i)

  private def decodeRow(bytes: Array[Byte], offsets: Array[Int], i: Int): String =
    val from = offsets(i)
    String(bytes, from, offsets(i + 1) - from, java.nio.charset.StandardCharsets.UTF_8)

  private def decodeBytes(
      bytes: Array[Byte],
      offsets: Array[Int],
      nulls: Array[Boolean],
      n: Int
  ): Array[String] =
    val out = new Array[String](n)
    var i   = 0
    while i < n do
      if nulls == null || !nulls(i) then
        out(i) = decodeRow(bytes, offsets, i)
      i += 1
    out

  private def gatherStrings(values: Array[String], idx: Array[Int], n: Int): Array[String] =
    val out = new Array[String](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    out

  private def decodeEntries(entries: Array[String], codes: Array[Int], n: Int): Array[String] =
    val out = new Array[String](n)
    var i   = 0
    while i < n do
      val c = codes(i)
      if c >= 0 then
        out(i) = entries(c)
      i += 1
    out

  /** The codes at the given positions, -1 for padding (idx -1) */
  private[vector] def gatherCodes(codes: Array[Int], idx: Array[Int], n: Int): Array[Int] =
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      out(i) =
        if k >= 0 then
          codes(k)
        else
          -1
      i += 1
    out

end StrVec

/** Boxed values: wide decimals (java.math.BigDecimal), and nested or untyped values */
final class ObjVec(val ptype: PType, val values: Array[AnyRef], val nulls: Array[Boolean])
    extends Vec:
  def size: Int        = values.length
  def get(i: Int): Any =
    if isNull(i) then
      null
    else
      values(i)

  def gather(idx: Array[Int], n: Int): Vec =
    val out = new Array[AnyRef](n)
    var i   = 0
    while i < n do
      val k = idx(i)
      if k >= 0 then
        out(i) = values(k)
      i += 1
    val outNulls = Vec.nullsWithPadding(nulls, idx, n)
    ObjVec(ptype, out, outNulls)

/**
  * Appends engine values of one type. Decimal builders start Long-backed and switch to BigDecimal
  * storage on the first value that does not fit.
  */
abstract class VecBuilder:
  def ptype: PType
  def add(v: Any): Unit
  def addNull(): Unit
  def size: Int
  def result(): Vec

object VecBuilder:
  def apply(ptype: PType, capacity: Int = 16): VecBuilder =
    ptype match
      case PType.Bool =>
        BoolBuilder(capacity)
      case PType.Int32 | PType.Date =>
        IntBuilder(ptype, capacity)
      case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
        LongBuilder(ptype, capacity)
      case d: PType.Decimal =>
        DecimalBuilder(d, capacity)
      case PType.Float64 =>
        DoubleBuilder(capacity)
      case PType.Str =>
        StrBuilder(capacity)
      case _ =>
        ObjBuilder(ptype, capacity)

  abstract class Base(capacity0: Int) extends VecBuilder:
    protected var n                     = 0
    protected var nulls: Array[Boolean] = null
    protected def capacity: Int
    protected def grow(newCap: Int): Unit
    def size: Int = n

    protected final def ensure(): Unit =
      if n >= capacity then
        val newCap = math.max(16, capacity * 2)
        grow(newCap)
        if nulls != null then
          nulls = java.util.Arrays.copyOf(nulls, newCap)

    def addNull(): Unit =
      ensure()
      if nulls == null then
        nulls = new Array[Boolean](capacity)
      nulls(n) = true
      n += 1

    protected final def resultNulls: Array[Boolean] =
      if nulls == null then
        null
      else
        java.util.Arrays.copyOf(nulls, n)

  final class BoolBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[Boolean](math.max(cap, 1))
    def ptype: PType                      = PType.Bool
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def add(v: Any): Unit                 =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = ValueOps.toBoolean(v)
        n += 1

    def result(): Vec = BoolVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class IntBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[Int](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addInt(v: Int): Unit              =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addInt(ValueOps.toInt(v))

    def result(): Vec = IntVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  final class LongBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[Long](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addLong(v: Long): Unit            =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addLong(ValueOps.toLong(v))

    def result(): Vec = LongVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  final class DoubleBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[Double](math.max(cap, 1))
    def ptype: PType                      = PType.Float64
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addDouble(v: Double): Unit        =
      ensure()
      arr(n) = v
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addDouble(ValueOps.toDouble(v))

    def result(): Vec = DoubleVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class StrBuilder(cap: Int) extends Base(cap):
    private var arr                       = new Array[String](math.max(cap, 1))
    def ptype: PType                      = PType.Str
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def addString(v: String): Unit        =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = v
        n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addString(ValueOps.toStr(v))

    def result(): Vec = StrVec(java.util.Arrays.copyOf(arr, n), resultNulls)

  final class ObjBuilder(val ptype: PType, cap: Int) extends Base(cap):
    private var arr                       = new Array[AnyRef](math.max(cap, 1))
    protected def capacity: Int           = arr.length
    protected def grow(newCap: Int): Unit = arr = java.util.Arrays.copyOf(arr, newCap)
    def add(v: Any): Unit                 =
      if v == null then
        addNull()
      else
        ensure()
        arr(n) = v.asInstanceOf[AnyRef]
        n += 1

    def result(): Vec = ObjVec(ptype, java.util.Arrays.copyOf(arr, n), resultNulls)

  /** Decimal values are rescaled to the builder's scale */
  final class DecimalBuilder(val ptype: PType.Decimal, cap: Int) extends Base(cap):
    private var longs: Array[Long] = new Array[Long](math.max(cap, 1))
    private var big: Array[AnyRef] = null
    protected def capacity: Int    =
      if big != null then
        big.length
      else
        longs.length

    protected def grow(newCap: Int): Unit =
      if big != null then
        big = java.util.Arrays.copyOf(big, newCap)
      else
        longs = java.util.Arrays.copyOf(longs, newCap)

    private def switchToBig(): Unit =
      val b = new Array[AnyRef](longs.length)
      var i = 0
      while i < n do
        b(i) = JBigDecimal.valueOf(longs(i), ptype.scale)
        i += 1
      big = b
      longs = null

    def addUnscaled(v: Long): Unit =
      ensure()
      if big != null then
        big(n) = JBigDecimal.valueOf(v, ptype.scale)
      else
        longs(n) = v
      n += 1

    def addBig(v: JBigDecimal): Unit =
      val d = ValueOps.rescale(v, ptype.scale)
      ensure()
      if big == null then
        if d.unscaledValue().bitLength() < 64 then
          longs(n) = d.unscaledValue().longValue()
          n += 1
          return
        switchToBig()
      big(n) = d
      n += 1

    def add(v: Any): Unit =
      if v == null then
        addNull()
      else
        addBig(ValueOps.toBigDecimal(v))

    def result(): Vec =
      if big != null then
        ObjVec(ptype, java.util.Arrays.copyOf(big, n), resultNulls)
      else
        LongVec(ptype, java.util.Arrays.copyOf(longs, n), resultNulls)

  end DecimalBuilder

end VecBuilder
