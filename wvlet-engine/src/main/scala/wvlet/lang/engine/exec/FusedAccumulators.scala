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

import wvlet.lang.engine.vector.Vec

/**
  * Folds the counts and decimal sums of one aggregation in a single pass over the rows of a batch
  * with few groups (`fused_accumulators`). Each accumulator on its own is one loop of
  * `state(groups(i)) += x`: with few groups, consecutive rows update the same slot, so the loop
  * waits on the store of the previous row. One loop updating every accumulator's slot per row lets
  * those independent updates overlap, and reads each row's group id once.
  *
  * Fused: `count(*)`, `count(x)` of a batch without NULLs, and decimal `sum(x)` of a batch without
  * NULLs whose values are unscaled Longs, all without FILTER. The rest update on their own. With
  * `column_major_fused_sums`, [[ColumnMajorSums]] folds them one accumulator at a time instead
  */
private[exec] object FusedAccumulators:

  /**
    * Batches fold fused when they have at least this many rows per group. A single-thread probe
    * (2048-row batches, uniform groups) gained 1.6x at 4 groups and 1.1x at 32-128, and lost from
    * 256 groups on, where rows rarely update the same slot one after another
    */
  private val FewGroupsRatio = 16

  /** Whether [[update]] may fold a batch of n rows in `numGroups` groups */
  def foldsBatch(numGroups: Int, n: Int): Boolean = numGroups.toLong * FewGroupsRatio <= n

  /**
    * Fold the rows of a batch into the fusable accumulators among `accs` (those with `fusable(a)`),
    * whose arguments are `args(a)`, and return which accumulators were folded, or null when fewer
    * than two can be (one accumulator gains nothing from the fused loop). Accumulators must have
    * room for `numGroups` groups. With `sel`, row i of the batch is row `sel(i)` of the arguments
    * (none of them -1, `dense_selected_rows`)
    */
  def update(
      accs: IndexedSeq[Accumulator],
      fusable: Array[Boolean],
      args: Array[Array[Vec]],
      groups: Array[Int],
      n: Int,
      numGroups: Int,
      sel: Array[Int] = null,
      columnMajor: Boolean = false
  ): Array[Boolean] =
    if !foldsBatch(numGroups, n) then
      return null
    val m      = accs.size
    val folded = new Array[Boolean](m)
    val counts = new Array[Array[Long]](m)
    val sums   = new Array[Aggregates.SumDecimal](m)
    val slots  = new Array[Array[Long]](m)
    val values = new Array[Array[Long]](m)
    var nc     = 0
    var ns     = 0
    var a      = 0
    while a < m do
      if fusable(a) then
        accs(a) match
          case c: Aggregates.CountStar =>
            counts(nc) = c.counts
            nc += 1
            folded(a) = true
          case c: Aggregates.CountNonNull if args(a)(0).nulls == null =>
            counts(nc) = c.counts
            nc += 1
            folded(a) = true
          case s: Aggregates.SumDecimal =>
            val u = s.fusableValues(args(a)(0))
            if u != null then
              sums(ns) = s
              slots(ns) = s.sums
              values(ns) = u
              ns += 1
              folded(a) = true
          case _ =>
      a += 1
    if nc + ns < 2 then
      null
    else
      if columnMajor then
        ColumnMajorSums.fold(counts, nc, sums, slots, values, ns, groups, n, numGroups, sel)
      else
        fewGroups(counts, nc, sums, slots, values, ns, groups, n, numGroups, sel)
      folded

  end update

  /**
    * The rows count into one array, whose per-group increase then goes to the other counts and
    * marks the groups the sums saw: the loop updates one count per row and no `seen` flags
    */
  private def fewGroups(
      counts: Array[Array[Long]],
      nc: Int,
      sums: Array[Aggregates.SumDecimal],
      slots: Array[Array[Long]],
      values: Array[Array[Long]],
      ns: Int,
      groups: Array[Int],
      n: Int,
      numGroups: Int,
      sel: Array[Int]
  ): Unit =
    // Without a count, a zeroed scratch array counts the batch's rows, which are then its increase
    val rows =
      if nc > 0 then
        counts(0)
      else
        new Array[Long](numGroups)
    val before =
      if nc > 0 then
        java.util.Arrays.copyOf(rows, numGroups)
      else
        null
    // Row i reads its values at r; the two loops keep the dense one free of the indirection
    inline def fold(i: Int, r: Int): Unit =
      val g = groups(i)
      rows(g) += 1
      var j = 0
      while j < ns do
        val s = slots(j)
        val x = s(g)
        val y = values(j)(r)
        val z = x + y
        if ((x ^ z) & (y ^ z)) < 0 then
          sums(j).spill(g, y)
        else
          s(g) = z
        j += 1
    var i = 0
    if sel == null then
      while i < n do
        fold(i, i)
        i += 1
    else
      while i < n do
        fold(i, sel(i))
        i += 1
    var g = 0
    while g < numGroups do
      val added =
        if before == null then
          rows(g)
        else
          rows(g) - before(g)
      if added != 0 then
        var c = 1
        while c < nc do
          counts(c)(g) += added
          c += 1
        var j = 0
        while j < ns do
          sums(j).seenFlags(g) = true
          j += 1
      g += 1

  end fewGroups

end FusedAccumulators
