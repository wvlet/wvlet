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
  * Packs a GROUP BY key of several Int32, Date, Int64, or Long-backed decimal columns into one
  * Int64 value, from each column's range over the rows being grouped: a column takes code 0 for
  * NULL and `v - min + 1` otherwise, in its own bits. Two keys are equal (NULLs equal to each
  * other, as GROUP BY groups them) exactly when their packed values are, so a [[GroupTable]] over
  * the packed column takes its single-Int64 path; [[unpack]] restores the key columns of the
  * groups.
  *
  * A column of `w` bits covers the values `min` to `min + 2^w - 2`, which can be more than the rows
  * it was built from: [[tryPack]] packs any batch within those ranges.
  */
final class AggKeyPacking private (
    types: IndexedSeq[PType],
    mins: Array[Long],
    shifts: Array[Int],
    widths: Array[Int]
):
  private val width = types.size

  /** The largest value `v - min` each column covers, up to Long.MaxValue */
  private val spans: Array[Long] =
    Array.tabulate(width) { k =>
      val s = (1L << widths(k)) - 2
      if mins(k) >= 0 && Long.MaxValue - mins(k) < s then
        Long.MaxValue - mins(k)
      else
        s
    }

  /** The packed key of each of the first n rows; every value must be within the packed ranges */
  def pack(keys: Array[Vec], n: Int): LongVec =
    val out = tryPack(keys, n)
    if out == null then
      throw IllegalStateException("A key is outside the packed ranges")
    out

  /**
    * The packed key of each of the first n rows, or null when a key value is outside the covered
    * ranges or a key column isn't in the representation of its type
    */
  def tryPack(keys: Array[Vec], n: Int): LongVec = tryPack(keys, null, n)

  /**
    * [[tryPack]] where key k is `keys(k)` at the positions `sels(k)` (-1 for NULL) when `sels` and
    * `sels(k)` are not null, reading the values in place
    */
  def tryPack(keys: Array[Vec], sels: Array[Array[Int]], n: Int): LongVec =
    val out = new Array[Long](n)
    var k   = 0
    while k < width do
      val v     = keys(k)
      val lo    = mins(k)
      val span  = spans(k)
      val shift = shifts(k)
      val vn    = v.nulls
      val sel   =
        if sels == null then
          null
        else
          sels(k)
      // Whether a value is outside [lo, lo + span]. `x - lo` can't wrap into that range: lo + span
      // doesn't overflow
      var miss = false
      var i    = 0
      // A NULL key (or position -1) adds nothing: 0 is the NULL code
      inline def packAt(inline value: Int => Long): Unit =
        while i < n do
          val r = sel(i)
          if r >= 0 && (vn == null || !vn(r)) then
            val c = value(r) - lo
            miss |= c < 0 || c > span
            out(i) |= (c + 1) << shift
          i += 1
      v match
        case iv: IntVec if sel != null && AggKeyPacking.isIntColumn(types(k)) =>
          val x = iv.values
          packAt(r => x(r).toLong)
        case lv: LongVec if sel != null && lv.ptype == types(k) =>
          val x = lv.values
          packAt(r => x(r))
        case iv: IntVec if AggKeyPacking.isIntColumn(types(k)) =>
          val x = iv.values
          while i < n do
            if vn == null || !vn(i) then
              val c = x(i).toLong - lo
              miss |= c < 0 || c > span
              out(i) |= (c + 1) << shift
            i += 1
        case lv: LongVec if lv.ptype == types(k) =>
          val x = lv.values
          while i < n do
            if vn == null || !vn(i) then
              val c = x(i) - lo
              miss |= c < 0 || c > span
              out(i) |= (c + 1) << shift
            i += 1
        case _ =>
          return null
      if miss then
        return null
      k += 1
    end while
    LongVec(PType.Int64, out, null)

  end tryPack

  /** The key columns of the first n packed keys */
  def unpack(packed: LongVec, n: Int): Array[Vec] =
    val p = packed.values
    Array.tabulate(width) { k =>
      val mask                  = (1L << widths(k)) - 1
      val lo                    = mins(k)
      val shift                 = shifts(k)
      var nulls: Array[Boolean] = null
      types(k) match
        case t @ (PType.Int32 | PType.Date) =>
          val out = new Array[Int](n)
          var i   = 0
          while i < n do
            val c = (p(i) >>> shift) & mask
            if c != 0 then
              out(i) = (c - 1 + lo).toInt
            else
              if nulls == null then
                nulls = new Array[Boolean](n)
              nulls(i) = true
            i += 1
          IntVec(t, out, nulls)
        case t =>
          // Int64 or a Long-backed decimal: the unscaled values
          val out = new Array[Long](n)
          var i   = 0
          while i < n do
            val c = (p(i) >>> shift) & mask
            if c != 0 then
              out(i) = c - 1 + lo
            else
              if nulls == null then
                nulls = new Array[Boolean](n)
              nulls(i) = true
            i += 1
          LongVec(t, out, nulls)
    }

  end unpack

end AggKeyPacking

object AggKeyPacking:
  /** Decimals up to this precision are always Long-backed (unscaled values) */
  val MaxLongDecimalPrecision = 18

  private def isIntColumn(t: PType): Boolean = t == PType.Int32 || t == PType.Date

  private def packableType(t: PType, decimals: Boolean): Boolean =
    t match
      case PType.Int32 | PType.Date | PType.Int64 =>
        true
      case d: PType.Decimal =>
        decimals && d.precision <= MaxLongDecimalPrecision
      case _ =>
        false

  /**
    * Whether keys of `types` can be packed: two or more Int32, Date, or Int64 columns, and with
    * `decimals`, decimals of precision up to 18 too
    */
  def packable(types: IndexedSeq[PType], decimals: Boolean = false): Boolean =
    types.size >= 2 && types.forall(packableType(_, decimals))

  /**
    * The packing of keys of `types` covering every row of `keyBatches` (each the key columns of one
    * batch, and its row count), or None when a key column isn't in the representation of its type
    * (an IntVec for Int32 or Date, a LongVec of the type for Int64 or a decimal), there are fewer
    * than two key columns, or the codes don't fit in 63 bits
    */
  def of(
      types: IndexedSeq[PType],
      keyBatches: Seq[(Array[Vec], Int)],
      decimals: Boolean = false
  ): Option[AggKeyPacking] =
    if !packable(types, decimals) then
      None
    else
      val mins = Array.fill(types.size)(Long.MaxValue)
      val maxs = Array.fill(types.size)(Long.MinValue)
      if keyBatches.forall((keys, n) => widen(types, keys, n, mins, maxs)) then
        fromRanges(types, mins, maxs)
      else
        None

  /**
    * A packing covering the first `groups._2` rows of `groups._1` (the keys grouped so far, when
    * given) and the first n rows of `keys`, with room for values about one range beyond each end of
    * every column when the codes still fit, so a grouping that streams its input repacks its groups
    * only a few times. The ranges are those of the actual values, so earlier room doesn't carry
    * over. None when a key column isn't in the representation of its type or the codes don't fit in
    * 63 bits
    */
  def covering(
      types: IndexedSeq[PType],
      groups: Option[(Array[Vec], Int)],
      keys: Array[Vec],
      n: Int,
      decimals: Boolean
  ): Option[AggKeyPacking] =
    if !packable(types, decimals) then
      return None
    val width = types.size
    val mins  = Array.fill(width)(Long.MaxValue)
    val maxs  = Array.fill(width)(Long.MinValue)
    if !groups.forall((g, count) => widen(types, g, count, mins, maxs)) then
      return None
    if !widen(types, keys, n, mins, maxs) then
      return None
    val wideMins = mins.clone()
    val wideMaxs = maxs.clone()
    var k        = 0
    while k < width do
      if mins(k) <= maxs(k) then
        val (lo, hi) =
          if isIntColumn(types(k)) then
            (Int.MinValue.toLong, Int.MaxValue.toLong)
          else
            (Long.MinValue, Long.MaxValue)
        // Ranges up to 2^62, so the slack never overflows
        val slack = math.min(maxs(k) - mins(k), 1L << 62)
        if slack >= 0 then
          wideMins(k) =
            if mins(k) >= lo + slack then
              mins(k) - slack
            else
              lo
          wideMaxs(k) =
            if maxs(k) <= hi - slack then
              maxs(k) + slack
            else
              hi
      k += 1
    fromRanges(types, wideMins, wideMaxs).orElse(fromRanges(types, mins, maxs))

  end covering

  /**
    * Widens mins and maxs to the non-NULL values of the first n rows of keys; false when a key
    * column isn't in the representation of its type
    */
  private def widen(
      types: IndexedSeq[PType],
      keys: Array[Vec],
      n: Int,
      mins: Array[Long],
      maxs: Array[Long]
  ): Boolean =
    var k = 0
    while k < types.size do
      val vn = keys(k).nulls
      var lo = mins(k)
      var hi = maxs(k)
      var i  = 0
      keys(k) match
        case v: IntVec if isIntColumn(types(k)) =>
          val x = v.values
          while i < n do
            if vn == null || !vn(i) then
              val a = x(i).toLong
              if a < lo then
                lo = a
              if a > hi then
                hi = a
            i += 1
        case v: LongVec if v.ptype == types(k) =>
          val x = v.values
          while i < n do
            if vn == null || !vn(i) then
              val a = x(i)
              if a < lo then
                lo = a
              if a > hi then
                hi = a
            i += 1
        case _ =>
          return false
      mins(k) = lo
      maxs(k) = hi
      k += 1
    end while
    true

  end widen

  /**
    * The packing of the value ranges [mins(k), maxs(k)], or None when the codes need over 63 bits
    */
  private def fromRanges(
      types: IndexedSeq[PType],
      lows: Array[Long],
      maxs: Array[Long]
  ): Option[AggKeyPacking] =
    val width  = types.size
    val mins   = lows.clone()
    val shifts = new Array[Int](width)
    val widths = new Array[Int](width)
    var used   = 0
    var k      = 0
    while k < width do
      if mins(k) > maxs(k) then
        // Only NULLs: code 0 alone
        mins(k) = 0
        widths(k) = 1
      else
        val range = maxs(k) - mins(k)
        if range < 0 || range >= Long.MaxValue - 1 then
          return None
        // Codes 0 (NULL) to range + 1
        widths(k) = 64 - java.lang.Long.numberOfLeadingZeros(range + 1)
      shifts(k) = used
      used += widths(k)
      if used > 63 then
        return None
      k += 1
    Some(AggKeyPacking(types, mins, shifts, widths))

  end fromRanges

end AggKeyPacking
