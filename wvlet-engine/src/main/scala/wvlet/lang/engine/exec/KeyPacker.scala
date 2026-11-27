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
  * Packs a multi-column integer key into one Int64 value, from each column's range of values: a
  * column takes `v - min` in its own bits, so two keys are equal exactly when their packed values
  * are, and a hash table over the packed column compares one Long per candidate instead of every
  * column (as DuckDB's compressed join keys).
  *
  * Rows with a NULL key column, or with a value outside its column's range, pack as NULL: they
  * cannot equal any key the ranges were taken from.
  */
final class KeyPacker private (mins: Array[Long], maxs: Array[Long], shifts: Array[Int]):
  private val width = mins.length

  /**
    * The packed key of each of the first n rows, packed in chunks of rows on the runner
    * (`parallel_join_keys`) unless it is [[TaskRunner.Sequential]]
    */
  def pack(keys: Array[Vec], n: Int, runner: TaskRunner = TaskRunner.Sequential): LongVec =
    val out = new Array[Long](n)
    if runner eq TaskRunner.Sequential then
      LongVec(PType.Int64, out, packRows(keys, 0, n, out, null))
    else
      // Chunks write their own rows of one shared NULL mask, dropped when no row is NULL
      val nulls = new Array[Boolean](n)
      val ch    = JoinBuildSteps.chunks(n, JoinBuildSteps.taskCount(runner))
      val any   = new Array[Boolean](ch.length - 1)
      JoinBuildSteps.runAll(ch.length - 1, runner) { c =>
        any(c) = packRows(keys, ch(c), ch(c + 1), out, nulls) != null
      }
      LongVec(
        PType.Int64,
        out,
        if any.contains(true) then
          nulls
        else
          null
      )

  /**
    * Packs rows `from until until` into `out` and marks the rows that pack as NULL in a mask over
    * all of `out`: `shared` when not null, otherwise one allocated at the first such row. Returns
    * the mask, or null when none of these rows is NULL
    */
  private def packRows(
      keys: Array[Vec],
      from: Int,
      until: Int,
      out: Array[Long],
      shared: Array[Boolean]
  ): Array[Boolean] =
    // `shared` rows are set only for NULL rows, so its presence alone does not mean a NULL row
    var nulls: Array[Boolean]         = null
    inline def markNull(i: Int): Unit =
      if nulls == null then
        nulls =
          if shared != null then
            shared
          else
            new Array[Boolean](out.length)
      nulls(i) = true
    var k = 0
    while k < width do
      val v     = keys(k)
      val lo    = mins(k)
      val hi    = maxs(k)
      val shift = shifts(k)
      val vn    = v.nulls
      var i     = from
      v match
        case iv: IntVec =>
          val x = iv.values
          while i < until do
            val a = x(i).toLong
            if (vn != null && vn(i)) || a < lo || a > hi then
              markNull(i)
            else
              out(i) |= (a - lo) << shift
            i += 1
        case lv: LongVec =>
          val x = lv.values
          while i < until do
            val a = x(i)
            if (vn != null && vn(i)) || a < lo || a > hi then
              markNull(i)
            else
              out(i) |= (a - lo) << shift
            i += 1
        case other =>
          // Unreached: the planner casts probe keys to the build key types
          while i < until do
            val a =
              other.get(i) match
                case x: Int =>
                  x.toLong
                case x: Long =>
                  x
                case _ =>
                  hi + 1
            if a < lo || a > hi then
              markNull(i)
            else
              out(i) |= (a - lo) << shift
            i += 1
      end match
      k += 1
    end while
    nulls

  end packRows

end KeyPacker

object KeyPacker:
  /** Packed keys stay non-negative and leave headroom below the sign bit */
  inline val MaxBits = 62

  private def packable(t: PType): Boolean = t == PType.Int32 || t == PType.Date || t == PType.Int64

  /**
    * A packer for keys of these types, with ranges taken from the given rows (whose key columns
    * have no NULLs), or null when the keys cannot be packed: a single column (which has its own
    * fast paths), a non-integer column, no rows, or ranges needing more than [[MaxBits]] bits. The
    * ranges are taken in chunks of rows on the runner (`parallel_join_keys`) unless it is
    * [[TaskRunner.Sequential]]
    */
  def of(
      types: IndexedSeq[PType],
      keys: Array[Vec],
      rows: Array[Int],
      n: Int,
      runner: TaskRunner = TaskRunner.Sequential
  ): KeyPacker =
    val width = types.size
    if width < 2 || n == 0 || !types.forall(packable) then
      return null
    if !(runner eq TaskRunner.Sequential) then
      val ranges = keys.map(JoinBuildSteps.keyRange(_, rows, n, runner))
      if ranges.contains(None) then
        return null
      return fromRanges(ranges.map(_.get))
    val mins   = new Array[Long](width)
    val maxs   = new Array[Long](width)
    val shifts = new Array[Int](width)
    var bits   = 0
    var k      = 0
    while k < width do
      var lo = Long.MaxValue
      var hi = Long.MinValue
      var j  = 0
      keys(k) match
        case iv: IntVec =>
          val x = iv.values
          while j < n do
            val a = x(rows(j)).toLong
            if a < lo then
              lo = a
            if a > hi then
              hi = a
            j += 1
        case lv: LongVec =>
          val x = lv.values
          while j < n do
            val a = x(rows(j))
            if a < lo then
              lo = a
            if a > hi then
              hi = a
            j += 1
        case _ =>
          return null
      val range = hi - lo
      // A negative range overflowed: the column spans more than 63 bits
      if range < 0 then
        return null
      shifts(k) = bits
      bits += 64 - java.lang.Long.numberOfLeadingZeros(range)
      if bits > MaxBits then
        return null
      mins(k) = lo
      maxs(k) = hi
      k += 1
    end while
    KeyPacker(mins, maxs, shifts)

  end of

  /** A packer for columns of these [min, max] ranges, or null when they need over [[MaxBits]] */
  private def fromRanges(ranges: Array[(Long, Long)]): KeyPacker =
    val shifts = new Array[Int](ranges.length)
    var bits   = 0
    var k      = 0
    while k < ranges.length do
      val range = ranges(k)._2 - ranges(k)._1
      if range < 0 then
        return null
      shifts(k) = bits
      bits += 64 - java.lang.Long.numberOfLeadingZeros(range)
      if bits > MaxBits then
        return null
      k += 1
    KeyPacker(ranges.map(_._1), ranges.map(_._2), shifts)

end KeyPacker
