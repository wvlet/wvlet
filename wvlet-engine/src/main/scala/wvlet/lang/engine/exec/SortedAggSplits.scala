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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * Splits of an aggregation's input batches into contiguous runs that share no group, for input
  * whose rows come in non-decreasing order of one integer key column with no NULL
  * (`sorted_agg_splits`). The partials of a scan sorted by a grouping key (TPC-H lineitem by
  * `l_orderkey`) come in part order, so they are sorted across parts too, and only the keys at the
  * parts' boundaries repeat.
  *
  * A split ends only between two batches whose boundary keys differ, so every group's rows fall in
  * one split, and the splits can be aggregated independently, as hash partitions are, without
  * hashing and scattering the rows.
  */
object SortedAggSplits:

  /**
    * Contiguous runs of `input`, about `target` of them with about the same row count, such that no
    * value of a grouping key column occurs in two runs. None when no key column (Int32, Int64,
    * date, or Long-backed decimal) is non-decreasing over all rows with no NULL and cuts at least
    * half of `target` runs (at least two); a sorted key with too few distinct values, such as a
    * year, would leave most tasks idle, so the next sorted key is tried
    */
  def of(input: IndexedSeq[Batch], keys: IndexedSeq[VExpr], target: Int): Option[Seq[Range]] =
    if input.size < 2 || target < 2 then
      return None
    var k = 0
    while k < keys.size do
      if isIntegral(keys(k).ptype) then
        val bounds = sortedBounds(input, keys(k))
        if bounds != null then
          val runs = cut(input, bounds, target)
          // A key with few distinct values cuts too few runs to keep the tasks busy
          if runs.size >= math.max(2, target / 2) then
            return Some(runs)
      k += 1
    None

  private def isIntegral(t: PType): Boolean =
    t match
      case PType.Int32 | PType.Int64 | PType.Date =>
        true
      case PType.Decimal(_, _) =>
        true
      case _ =>
        false

  /**
    * The first and last key of each batch (first(i), last(i) at 2i and 2i+1) when the key is
    * non-decreasing over all rows with no NULL, or null
    */
  private def sortedBounds(input: IndexedSeq[Batch], key: VExpr): Array[Long] =
    val bounds = new Array[Long](input.size * 2)
    var prev   = Long.MinValue
    var i      = 0
    while i < input.size do
      val b = input(i)
      val n = b.size
      if n == 0 then
        bounds(2 * i) = prev
        bounds(2 * i + 1) = prev
      else
        key.eval(b) match
          case v: LongVec =>
            val x = v.values
            if hasNull(v.nulls, n) || x(0) < prev then
              return null
            var j = 1
            while j < n do
              if x(j) < x(j - 1) then
                return null
              j += 1
            bounds(2 * i) = x(0)
            bounds(2 * i + 1) = x(n - 1)
          case v: IntVec =>
            val x = v.values
            if hasNull(v.nulls, n) || x(0) < prev then
              return null
            var j = 1
            while j < n do
              if x(j) < x(j - 1) then
                return null
              j += 1
            bounds(2 * i) = x(0)
            bounds(2 * i + 1) = x(n - 1)
          case _ =>
            return null
        prev = bounds(2 * i + 1)
      end if
      i += 1
    end while
    bounds

  end sortedBounds

  private def hasNull(nulls: Array[Boolean], n: Int): Boolean =
    if nulls == null then
      false
    else
      var j = 0
      while j < n do
        if nulls(j) then
          return true
        j += 1
      false

  /**
    * Cut the batches into runs of about total / target rows, ending a run only where the key of a
    * batch's last row differs from the next batch's first
    */
  private def cut(input: IndexedSeq[Batch], bounds: Array[Long], target: Int): Seq[Range] =
    val total = input.iterator.map(_.size.toLong).sum
    val step  = math.max(1L, total / target)
    val runs  = Seq.newBuilder[Range]
    var start = 0
    var rows  = 0L
    var i     = 0
    while i < input.size - 1 do
      rows += input(i).size
      if rows >= step && bounds(2 * i + 1) != bounds(2 * (i + 1)) then
        runs += (start to i)
        start = i + 1
        rows = 0L
      i += 1
    runs += (start until input.size)
    runs.result()

end SortedAggSplits
