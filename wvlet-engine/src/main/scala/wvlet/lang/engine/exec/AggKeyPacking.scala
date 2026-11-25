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
  * Packs a GROUP BY key of several Int32, Date, or Int64 columns into one Int64 value, from each
  * column's range over the rows being grouped: a column takes code 0 for NULL and `v - min + 1`
  * otherwise, in its own bits. Two keys are equal (NULLs equal to each other, as GROUP BY groups
  * them) exactly when their packed values are, so a [[GroupTable]] over the packed column takes its
  * single-Int64 path; [[unpack]] restores the key columns of the groups.
  */
final class AggKeyPacking private (
    types: IndexedSeq[PType],
    mins: Array[Long],
    shifts: Array[Int],
    widths: Array[Int]
):
  private val width = types.size

  /** The packed key of each of the first n rows; every value must be within the packed ranges */
  def pack(keys: Array[Vec], n: Int): LongVec =
    val out = new Array[Long](n)
    var k   = 0
    while k < width do
      val v     = keys(k)
      val lo    = mins(k)
      val shift = shifts(k)
      val vn    = v.nulls
      var i     = 0
      v match
        case iv: IntVec =>
          val x = iv.values
          while i < n do
            if vn == null || !vn(i) then
              out(i) |= (x(i).toLong - lo + 1) << shift
            i += 1
        case lv: LongVec =>
          val x = lv.values
          while i < n do
            if vn == null || !vn(i) then
              out(i) |= (x(i) - lo + 1) << shift
            i += 1
        case _ =>
          throw IllegalStateException(s"Unexpected key vector ${v.getClass.getSimpleName}")
      k += 1
    LongVec(PType.Int64, out, null)

  /** The key columns of the first n packed keys */
  def unpack(packed: LongVec, n: Int): Array[Vec] =
    val p = packed.values
    Array.tabulate(width) { k =>
      val mask                  = (1L << widths(k)) - 1
      val lo                    = mins(k)
      val shift                 = shifts(k)
      var nulls: Array[Boolean] = null
      def code(i: Int): Long    =
        val c = (p(i) >>> shift) & mask
        if c == 0 then
          if nulls == null then
            nulls = new Array[Boolean](n)
          nulls(i) = true
        c
      types(k) match
        case t @ (PType.Int32 | PType.Date) =>
          val out = new Array[Int](n)
          var i   = 0
          while i < n do
            val c = code(i)
            if c != 0 then
              out(i) = (c - 1 + lo).toInt
            i += 1
          IntVec(t, out, nulls)
        case t =>
          val out = new Array[Long](n)
          var i   = 0
          while i < n do
            val c = code(i)
            if c != 0 then
              out(i) = c - 1 + lo
            i += 1
          LongVec(t, out, nulls)
    }

  end unpack

end AggKeyPacking

object AggKeyPacking:
  /**
    * The packing of keys of `types` covering every row of `keyBatches` (each the key columns of one
    * batch, and its row count), or None when a key column isn't an Int32, Date, or Int64 vector,
    * there are fewer than two key columns, or the codes don't fit in 63 bits
    */
  def of(types: IndexedSeq[PType], keyBatches: Seq[(Array[Vec], Int)]): Option[AggKeyPacking] =
    val width = types.size
    val ok    =
      width >= 2 &&
        types.forall {
          case PType.Int32 | PType.Date | PType.Int64 =>
            true
          case _ =>
            false
        }
    if !ok then
      return None
    val mins    = Array.fill(width)(Long.MaxValue)
    val maxs    = Array.fill(width)(Long.MinValue)
    val batches = keyBatches.iterator
    while batches.hasNext do
      val (keys, n) = batches.next()
      var k         = 0
      while k < width do
        val vn = keys(k).nulls
        var lo = mins(k)
        var hi = maxs(k)
        var i  = 0
        keys(k) match
          case v: IntVec if types(k) != PType.Int64 =>
            val x = v.values
            while i < n do
              if vn == null || !vn(i) then
                val a = x(i).toLong
                if a < lo then
                  lo = a
                if a > hi then
                  hi = a
              i += 1
          case v: LongVec if types(k) == PType.Int64 =>
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
            return None
        mins(k) = lo
        maxs(k) = hi
        k += 1
      end while
    end while
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

  end of

end AggKeyPacking
