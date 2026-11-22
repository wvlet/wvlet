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

/**
  * A stable sort of row indexes in an `Array[Int]`, compared with [[SortOp.RowComparator]]'s
  * `compareRows`, so the indexes are never boxed. Equal rows keep their relative order, which gives
  * the same result as `java.util.Arrays.sort` over boxed indexes with the same comparator.
  */
object IndexSort:
  /** Runs up to this length are sorted by insertion before merging */
  private final val InsertionMax = 32

  /** Sorts `a(from until until)` in place */
  def sort(a: Array[Int], from: Int, until: Int, cmp: SortOp.RowComparator): Unit =
    val n = until - from
    if n > 1 then
      if n <= InsertionMax then
        insertionSort(a, from, until, cmp)
      else
        val buf = new Array[Int](n)
        mergeSort(a, buf, from, until, cmp)

  /** Sorts all of `a` in place */
  def sort(a: Array[Int], cmp: SortOp.RowComparator): Unit = sort(a, 0, a.length, cmp)

  private def insertionSort(a: Array[Int], from: Int, until: Int, cmp: SortOp.RowComparator): Unit =
    var i = from + 1
    while i < until do
      val x = a(i)
      var j = i - 1
      // Strictly greater only, so equal rows stay in order
      while j >= from && cmp.compareRows(a(j), x) > 0 do
        a(j + 1) = a(j)
        j -= 1
      a(j + 1) = x
      i += 1

  /**
    * Bottom-up merge sort: insertion-sorted runs of `InsertionMax`, then merges of doubling width,
    * alternating between `a` and `buf` (`buf(k - from)` mirrors `a(k)`)
    */
  private def mergeSort(
      a: Array[Int],
      buf: Array[Int],
      from: Int,
      until: Int,
      cmp: SortOp.RowComparator
  ): Unit =
    val n = until - from
    var s = from
    while s < until do
      insertionSort(a, s, math.min(s + InsertionMax, until), cmp)
      s += InsertionMax
    var src    = a
    var srcOff = from
    var dst    = buf
    var dstOff = 0
    var width  = InsertionMax
    while width < n do
      var lo = 0
      while lo < n do
        val mid = math.min(lo + width, n)
        val hi  = math.min(lo + 2 * width, n)
        var i   = lo
        var j   = mid
        var k   = lo
        // Skip the merge when the two runs are already in order
        if mid < hi && cmp.compareRows(src(srcOff + mid - 1), src(srcOff + mid)) <= 0 then
          System.arraycopy(src, srcOff + lo, dst, dstOff + lo, hi - lo)
        else
          while i < mid && j < hi do
            // Ties take the left run first, which keeps the sort stable
            if cmp.compareRows(src(srcOff + j), src(srcOff + i)) < 0 then
              dst(dstOff + k) = src(srcOff + j)
              j += 1
            else
              dst(dstOff + k) = src(srcOff + i)
              i += 1
            k += 1
          if i < mid then
            System.arraycopy(src, srcOff + i, dst, dstOff + k, mid - i)
          else if j < hi then
            System.arraycopy(src, srcOff + j, dst, dstOff + k, hi - j)
        lo += 2 * width
      val t  = src
      val to = srcOff
      src = dst
      srcOff = dstOff
      dst = t
      dstOff = to
      width *= 2
    end while
    if src ne a then
      System.arraycopy(src, srcOff, a, from, n)

  end mergeSort

end IndexSort
