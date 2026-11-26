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
  * The fused fold of [[FusedAccumulators]] one accumulator at a time (`column_major_fused_sums`).
  * The row-major fold updates every accumulator per row, which reloads each accumulator's arrays
  * and checks each sum for overflow per row. Here each sum is one tight loop over the group ids:
  *
  *   - Rows add into [[Lanes]] partial sums per group, row i into lane `i % Lanes`, so that
  *     consecutive rows of the same group (with few groups, most of them) update different slots
  *     and don't wait on each other's store.
  *   - The loop checks no overflow per row. It ORs the values' magnitudes, and when they bound the
  *     batch's sum within a Long, the lanes' total adds to each group's sum with one overflow check
  *     ([[Aggregates.SumDecimal.spill]] on overflow). A batch of larger values adds row by row with
  *     the per-row check instead.
  *   - The rows count once per group, and add to each count; groups that got rows mark the sums
  *     seen.
  */
private[exec] object ColumnMajorSums:

  /** Partial sums per group: rows rotate over them */
  private val Lanes = 4

  /** Fold the rows into the counts and sums, as [[FusedAccumulators.update]] describes */
  def fold(
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
    val part  = new Array[Long](Lanes * numGroups)
    val added = rowsPerGroup(part, groups, n, numGroups)
    var c     = 0
    while c < nc do
      val cs = counts(c)
      var g  = 0
      while g < numGroups do
        cs(g) += added(g)
        g += 1
      c += 1
    var j = 0
    while j < ns do
      if j > 0 then
        java.util.Arrays.fill(part, 0L)
      val s = slots(j)
      val v = values(j)
      val m =
        if sel == null then
          laneSums(part, v, groups, n, numGroups)
        else
          laneSumsAt(part, v, sel, groups, n, numGroups)
      if bounded(m, n) then
        addLanes(sums(j), s, part, added, numGroups)
      else
        rowByRow(sums(j), s, v, sel, groups, n)
      val seen = sums(j).seenFlags
      var g    = 0
      while g < numGroups do
        if added(g) != 0 then
          seen(g) = true
        g += 1
      j += 1

  end fold

  /**
    * The rows of each group, counted in the lanes of `part` (left zeroed for the sums), and the
    * returned array
    */
  private def rowsPerGroup(
      part: Array[Long],
      groups: Array[Int],
      n: Int,
      numGroups: Int
  ): Array[Long] =
    val g1 = numGroups
    val g2 = 2 * numGroups
    val g3 = 3 * numGroups
    val e  = n & ~(Lanes - 1)
    var i  = 0
    while i < e do
      part(groups(i)) += 1
      part(g1 + groups(i + 1)) += 1
      part(g2 + groups(i + 2)) += 1
      part(g3 + groups(i + 3)) += 1
      i += Lanes
    while i < n do
      part(groups(i)) += 1
      i += 1
    val added = new Array[Long](numGroups)
    var g     = 0
    while g < numGroups do
      added(g) = part(g) + part(g1 + g) + part(g2 + g) + part(g3 + g)
      g += 1
    java.util.Arrays.fill(part, 0L)
    added

  end rowsPerGroup

  // Below |y| + 1 (|y| for y >= 0): their OR bounds the values' magnitudes
  private inline def magnitude(y: Long): Long = y ^ (y >> 63)

  /**
    * Whether n values whose magnitudes OR to m sum within a Long in any order: each is at most
    * 2^bits(m), so any partial sum stays below 2^(bits(m) + bits(n)), here at most 2^62
    */
  private[exec] def bounded(m: Long, n: Int): Boolean =
    val bits  = 64 - java.lang.Long.numberOfLeadingZeros(m)
    val nbits = 32 - Integer.numberOfLeadingZeros(n)
    bits + 1 + nbits <= 63

  /** Add value i into lane `i % Lanes` of its group; the OR of the values' magnitudes */
  private def laneSums(
      part: Array[Long],
      v: Array[Long],
      groups: Array[Int],
      n: Int,
      numGroups: Int
  ): Long =
    val g1 = numGroups
    val g2 = 2 * numGroups
    val g3 = 3 * numGroups
    val e  = n & ~(Lanes - 1)
    var m  = 0L
    var i  = 0
    while i < e do
      val y0 = v(i)
      val y1 = v(i + 1)
      val y2 = v(i + 2)
      val y3 = v(i + 3)
      part(groups(i)) += y0
      part(g1 + groups(i + 1)) += y1
      part(g2 + groups(i + 2)) += y2
      part(g3 + groups(i + 3)) += y3
      m |= magnitude(y0) | magnitude(y1) | magnitude(y2) | magnitude(y3)
      i += Lanes
    while i < n do
      val y = v(i)
      part(groups(i)) += y
      m |= magnitude(y)
      i += 1
    m

  end laneSums

  /** [[laneSums]] with row i's value at `sel(i)` */
  private def laneSumsAt(
      part: Array[Long],
      v: Array[Long],
      sel: Array[Int],
      groups: Array[Int],
      n: Int,
      numGroups: Int
  ): Long =
    val g1 = numGroups
    val g2 = 2 * numGroups
    val g3 = 3 * numGroups
    val e  = n & ~(Lanes - 1)
    var m  = 0L
    var i  = 0
    while i < e do
      val y0 = v(sel(i))
      val y1 = v(sel(i + 1))
      val y2 = v(sel(i + 2))
      val y3 = v(sel(i + 3))
      part(groups(i)) += y0
      part(g1 + groups(i + 1)) += y1
      part(g2 + groups(i + 2)) += y2
      part(g3 + groups(i + 3)) += y3
      m |= magnitude(y0) | magnitude(y1) | magnitude(y2) | magnitude(y3)
      i += Lanes
    while i < n do
      val y = v(sel(i))
      part(groups(i)) += y
      m |= magnitude(y)
      i += 1
    m

  end laneSumsAt

  /** Add each group's lane total to its sum, spilling on overflow */
  private def addLanes(
      sum: Aggregates.SumDecimal,
      s: Array[Long],
      part: Array[Long],
      added: Array[Long],
      numGroups: Int
  ): Unit =
    val g1 = numGroups
    val g2 = 2 * numGroups
    val g3 = 3 * numGroups
    var g  = 0
    while g < numGroups do
      if added(g) != 0 then
        val y = part(g) + part(g1 + g) + part(g2 + g) + part(g3 + g)
        val x = s(g)
        val z = x + y
        // Overflow iff both operands have the same sign and the result's sign differs
        if ((x ^ z) & (y ^ z)) < 0 then
          sum.spill(g, y)
        else
          s(g) = z
      g += 1

  /** Add the values row by row with the per-row overflow check, for values too large to bound */
  private def rowByRow(
      sum: Aggregates.SumDecimal,
      s: Array[Long],
      v: Array[Long],
      sel: Array[Int],
      groups: Array[Int],
      n: Int
  ): Unit =
    var i = 0
    while i < n do
      val g = groups(i)
      val y =
        if sel == null then
          v(i)
        else
          v(sel(i))
      val x = s(g)
      val z = x + y
      if ((x ^ z) & (y ^ z)) < 0 then
        sum.spill(g, y)
      else
        s(g) = z
      i += 1

end ColumnMajorSums
