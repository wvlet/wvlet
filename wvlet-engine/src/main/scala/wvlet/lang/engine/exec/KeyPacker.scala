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
      var mask: Array[Boolean] = null
      packRows(keys, 0, n, out) { () =>
        if mask == null then
          mask = new Array[Boolean](n)
        mask
      }
      LongVec(PType.Int64, out, mask)
    else
      // Chunks share one NULL mask, allocated by the first chunk with a NULL row
      val mask = java.util.concurrent.atomic.AtomicReference[Array[Boolean]]()
      val ch   = JoinBuildSteps.chunks(n, JoinBuildSteps.taskCount(runner))
      JoinBuildSteps.runAll(ch.length - 1, runner) { c =>
        packRows(keys, ch(c), ch(c + 1), out) { () =>
          if mask.get() == null then
            mask.compareAndSet(null, new Array[Boolean](n))
          mask.get()
        }
      }
      LongVec(PType.Int64, out, mask.get())

  /**
    * Packs rows `from until until` into `out`, and marks the rows that pack as NULL in the mask
    * over all of `out` that `maskOf` gives, asked for only at the first such row
    */
  private def packRows(keys: Array[Vec], from: Int, until: Int, out: Array[Long])(
      maskOf: () => Array[Boolean]
  ): Unit =
    var nulls: Array[Boolean]         = null
    inline def markNull(i: Int): Unit =
      if nulls == null then
        nulls = maskOf()
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
    val ranges = keys.map(JoinBuildSteps.keyRange(_, rows, n, runner))
    if ranges.contains(None) then
      null
    else
      fromRanges(ranges.map(_.get))

  end of

  /** A packer for columns of these [min, max] ranges, or null when they need over [[MaxBits]] */
  private def fromRanges(ranges: Array[(Long, Long)]): KeyPacker =
    val shifts = new Array[Int](ranges.length)
    var bits   = 0
    var k      = 0
    while k < ranges.length do
      val range = ranges(k)._2 - ranges(k)._1
      // A negative range overflowed: the column spans more than 63 bits
      if range < 0 then
        return null
      shifts(k) = bits
      bits += 64 - java.lang.Long.numberOfLeadingZeros(range)
      if bits > MaxBits then
        return null
      k += 1
    KeyPacker(ranges.map(_._1), ranges.map(_._2), shifts)

end KeyPacker
