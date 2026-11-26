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
  * Aggregates (`members`, positions in the aggregation's calls) whose FILTERs are each a
  * [[PerSharedDictEntry]] over the string column `column` (`routed_agg_filters`). When a batch's
  * column holds codes into a dictionary shared across batches, and no code passes two of the
  * FILTERs, one pass over the codes routes each row to the aggregate whose FILTER it passes: the
  * rows of each are folded at once, read in place where the accumulator allows
  * ([[Accumulator.updateSelected]]). TPC-DS q59's seven `sum(ss_sales_price) filter (where
  * d_day_name = '...')` otherwise make seven masks over the rows, count them and gather each.
  *
  * Used by one aggregation operator at a time: the scratch arrays are reused across batches.
  */
final class RoutedAggFilters private (
    aggs: IndexedSeq[AggCall],
    members: Array[Int],
    column: Int,
    filters: Array[PerSharedDictEntry]
):
  // Members with the same FILTER (an avg split into a sum and a count) share one slot of rows
  private val slotSigs: IndexedSeq[String]     = filters.map(_.sig).distinct.toIndexedSeq
  private val slotOf: Array[Int]               = filters.map(f => slotSigs.indexOf(f.sig))
  private val conds: Array[PerSharedDictEntry] =
    slotSigs.map(sig => filters(filters.indexWhere(_.sig == sig))).toArray

  // Members with the same arguments share one evaluation of them per batch
  private val argSigs: Array[String] = members.map(a => aggs(a).args.map(_.sig).mkString(","))
  private val argSlot: Array[Int]    = argSigs.map(argSigs.indexOf(_))
  private val argVecs                = new Array[Array[Vec]](members.length)

  // The first shared dictionary seen, and for each of its codes (and NULL at dict.length) the
  // slot whose FILTER it passes (-1 for none); a null route when a code passes two FILTERs
  private var routeDict: Array[String] = null
  private var route: Array[Int]        = null

  private val handled = new Array[Boolean](aggs.size)
  private val counts  = new Array[Int](conds.length)
  private val idx     = Array.fill(conds.length)(Array.emptyIntArray)
  private val gs      = Array.fill(conds.length)(Array.emptyIntArray)
  members.foreach(handled(_) = true)

  /**
    * Fold the rows of `b` into the members' accumulators, and return which aggregates were folded
    * (the members); null when the batch doesn't route, so that each aggregate folds on its own
    */
  def update(
      accs: IndexedSeq[Accumulator],
      groups: Array[Int],
      numGroups: Int,
      b: Batch
  ): Array[Boolean] =
    b.column(column) match
      case s: StrVec if s.dict != null && s.hasCodes =>
        val r = routeFor(s.dict, b.size)
        if r == null then
          null
        else
          fold(r, s.dict.length, s.codes, accs, groups, numGroups, b)
          handled
      case _ =>
        null

  private def routeFor(dict: Array[String], rows: Int): Array[Int] =
    if routeDict != null then
      if routeDict eq dict then
        route
      else
        null
    else
      val results = conds.map(_.resultsFor(dict, rows))
      if results.exists(_ == null) then
        // Not cached by the FILTERs (another dictionary, or too large for this batch): no routing
        null
      else
        val m       = dict.length + 1
        val r       = Array.fill(m)(-1)
        var c       = 0
        var overlap = false
        while c < m && !overlap do
          var k = 0
          while k < results.length do
            if results(k).values(c) then
              if r(c) >= 0 then
                overlap = true
              r(c) = k
            k += 1
          c += 1
        routeDict = dict
        route =
          if overlap then
            null
          else
            r
        route

  private def fold(
      r: Array[Int],
      nullAt: Int,
      codes: Array[Int],
      accs: IndexedSeq[Accumulator],
      groups: Array[Int],
      numGroups: Int,
      b: Batch
  ): Unit =
    val n = b.size
    java.util.Arrays.fill(counts, 0)
    var i = 0
    while i < n do
      val c = codes(i)
      val k = r(
        if c >= 0 then
          c
        else
          nullAt
      )
      if k >= 0 then
        counts(k) += 1
      i += 1
    var k = 0
    while k < conds.length do
      if idx(k).length < counts(k) then
        idx(k) = new Array[Int](counts(k))
        gs(k) = new Array[Int](counts(k))
      counts(k) = 0
      k += 1
    i = 0
    while i < n do
      val c = codes(i)
      val t = r(
        if c >= 0 then
          c
        else
          nullAt
      )
      if t >= 0 then
        val j = counts(t)
        idx(t)(j) = i
        gs(t)(j) = groups(i)
        counts(t) = j + 1
      i += 1
    // Arguments evaluated once per distinct signature: the members often aggregate one column
    java.util.Arrays.fill(argVecs.asInstanceOf[Array[AnyRef]], null)
    def argsOf(m: Int): Array[Vec] =
      val a = argSlot(m)
      if argVecs(a) == null then
        argVecs(a) = aggs(members(m)).args.map(_.eval(b)).toArray
      argVecs(a)
    var m = 0
    while m < members.length do
      val acc = accs(members(m))
      val t   = slotOf(m)
      val cnt = counts(t)
      acc.ensureGroups(numGroups)
      if cnt == n then
        acc.update(groups, n, argsOf(m))
      else if cnt > 0 then
        val args = argsOf(m)
        if !(args.length == 1 && acc.updateSelected(gs(t), cnt, args(0), idx(t))) then
          acc.update(gs(t), cnt, args.map(_.gather(idx(t), cnt)))
      m += 1

  end fold

end RoutedAggFilters

object RoutedAggFilters:
  /**
    * The router over the largest set (at least two) of `aggs` marked `routedFilter` whose FILTERs
    * evaluate as one [[PerSharedDictEntry]] over the same column; null when there is none
    */
  def of(aggs: IndexedSeq[AggCall]): RoutedAggFilters =
    val candidates = aggs
      .indices
      .flatMap { a =>
        val c = aggs(a)
        if c.routedFilter && !c.distinct && c.order.isEmpty then
          c.evalFilter match
            case Some(p: PerSharedDictEntry) =>
              Some((a, p))
            case _ =>
              None
        else
          None
      }
    val byColumn = candidates.groupBy(_._2.column)
    if byColumn.isEmpty then
      null
    else
      val (column, best) = byColumn.maxBy((c, ms) => (ms.size, -c))
      if best.size < 2 then
        null
      else
        RoutedAggFilters(aggs, best.map(_._1).toArray, column, best.map(_._2).toArray)

  /** [[RoutedAggFilters.update]] of `router`, or null without one */
  def update(
      router: RoutedAggFilters,
      accs: IndexedSeq[Accumulator],
      groups: Array[Int],
      numGroups: Int,
      b: Batch
  ): Array[Boolean] =
    if router == null then
      null
    else
      router.update(accs, groups, numGroups, b)

end RoutedAggFilters
