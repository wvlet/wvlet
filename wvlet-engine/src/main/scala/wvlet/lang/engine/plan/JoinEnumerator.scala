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
package wvlet.lang.engine.plan

import scala.collection.mutable.ListBuffer

/**
  * Exact join order enumeration over a [[JoinGraph]] of at most [[JoinEnumerator.MaxInputs]]
  * inputs, including bushy trees. Like DPccp/DPhyp, it costs every pair of connected input sets
  * joined by an equality (a csg-cmp pair), where an equality whose sides read several inputs is a
  * hyperedge. Sets are visited in increasing bitset order, so the plans of both halves of a pair
  * are final before the pair is costed. Cross joins are not enumerated: a graph that needs one is
  * left to the greedy search.
  *
  * Estimates are numbers only (input rows and the distinct count of each equality side, read once
  * from its input), and the plan of the winning tree alone is built. The cost of a tree is the sum
  * over its joins of the output rows (C_out), the build rows, and the build rows again when the
  * build side is itself a join, as that breaks the probe pipeline; so left-deep probe chains win
  * ties.
  */
class JoinEnumerator(options: OptimizerOptions):
  import JoinEnumerator.*

  /**
    * The cheapest join tree and the conditions left to apply over it (those reading no input), or
    * None when the graph is too large, needs a cross join, or exceeds [[PairBudget]]
    */
  def plan[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): Option[(P, List[C])] =
    val n = g.inputs.size
    if n < 2 || n > MaxInputs then
      None
    else
      solve(g, search).map(dp => (build(g, search, dp, (1 << n) - 1), constants(g)))

  /** The best join of each input set, by bitset, or None if the full set has no plan */
  private[plan] def solve[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): Option[Table] =
    val n     = g.inputs.size
    val full  = (1 << n) - 1
    val edges = analyze(g, search)
    val dp    = Table(1 << n)
    for i <- 0 until n do
      dp.rows(1 << i) = g.rows(i)
      dp.planned(1 << i) = true
    var pairs = 0
    var s     = 3
    while s <= full && pairs <= PairBudget do
      if Integer.bitCount(s) > 1 then
        // Halves containing the lowest input of s, so that each unordered pair is visited once
        val low = Integer.lowestOneBit(s)
        val ext = s & ~low
        var sub = ext
        while sub != 0 && pairs <= PairBudget do
          val s1 = (s & ~sub)
          val s2 = sub
          if dp.planned(s1) && dp.planned(s2) && connects(edges, s1, s2) then
            pairs += 1
            emit(dp, edges, s1, s2)
          sub = (sub - 1) & ext
      s += 1
    Option.when(pairs <= PairBudget && dp.planned(full))(dp)

  end solve

  /** Cost the joins of s1 and s2 in both orders, keeping the cheapest plan of their union */
  private def emit(dp: Table, edges: IndexedSeq[Edge], s1: Int, s2: Int): Unit =
    val s       = s1 | s2
    val r1      = dp.rows(s1)
    val r2      = dp.rows(s2)
    val d1      = IndexedSeq.newBuilder[Option[Double]]
    val d2      = IndexedSeq.newBuilder[Option[Double]]
    var filters = 0
    edges.foreach { e =>
      if e.isEqui && within(e.left, s1) && within(e.right, s2) then
        d1 += e.leftDistinct
        d2 += e.rightDistinct
      else if e.isEqui && within(e.left, s2) && within(e.right, s1) then
        d1 += e.rightDistinct
        d2 += e.leftDistinct
      else if e.refs != 0 && placedAt(e.refs, s, s1, s2) then
        filters += 1
    }
    var out = CardinalityEstimator.joinOutputRows(r1, d1.result(), r2, d2.result())
    if filters > 0 then
      out = math.max(1.0, out * math.pow(CardinalityEstimator.DefaultSelectivity, filters))
    val base                                  = dp.cost(s1) + dp.cost(s2) + out
    def consider(left: Int, right: Int): Unit =
      val buildLeft = JoinOrderOptimizer.swapBuild(options, dp.rows(right), dp.rows(left))
      val build     =
        if buildLeft then
          left
        else
          right
      val buildRows     = dp.rows(build)
      val pipelineBreak =
        if Integer.bitCount(build) > 1 then
          buildRows
        else
          0.0
      val cost = base + buildRows + pipelineBreak
      if !dp.planned(s) || cost < dp.cost(s) then
        dp.planned(s) = true
        dp.cost(s) = cost
        dp.rows(s) = out
        dp.left(s) = left
        dp.right(s) = right
        dp.buildLeft(s) = buildLeft
    consider(s1, s2)
    consider(s2, s1)

  end emit

  /** The plan of the best join tree of input set s */
  private def build[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K],
      dp: Table,
      s: Int
  ): P =
    if Integer.bitCount(s) == 1 then
      g.inputs(Integer.numberOfTrailingZeros(s))
    else
      val l       = dp.left(s)
      val r       = dp.right(s)
      val left    = build(g, search, dp, l)
      val right   = build(g, search, dp, r)
      val pairs   = ListBuffer.empty[(C, C)]
      val filters = ListBuffer.empty[C]
      g.edges
        .foreach { e =>
          val refs = mask(e.refs)
          e.sides match
            case Some(sd) if isKey(sd, l, r) =>
              pairs += ((sd.left, sd.right))
            case Some(sd) if isKey(sd, r, l) =>
              pairs += ((sd.right, sd.left))
            case _ if refs != 0 && placedAt(refs, s, l, r) =>
              filters += e.cond
            case _ =>
        }
      val keys = search.keys(left, right, pairs.toList)
      val join = search.join(left, right, keys, dp.buildLeft(s))
      if filters.isEmpty then
        join
      else
        search.filter(join, filters.toList)

  /** The conditions reading no input, which no join places */
  private def constants[P, C](g: JoinGraph[P, C]): List[C] = g
    .edges
    .filter(_.refs.isEmpty)
    .map(_.cond)

end JoinEnumerator

object JoinEnumerator:
  /** The most inputs a graph may have for exact enumeration */
  val MaxInputs: Int = 10

  /**
    * The most csg-cmp pairs to cost, as in DuckDB; denser graphs are left to the greedy search
    */
  val PairBudget: Int = 10000

  /** Best plans by input bitset */
  private[plan] class Table(size: Int):
    val planned: Array[Boolean]   = Array.ofDim(size)
    val rows: Array[Double]       = Array.ofDim(size)
    val cost: Array[Double]       = Array.ofDim(size)
    val left: Array[Int]          = Array.ofDim(size)
    val right: Array[Int]         = Array.ofDim(size)
    val buildLeft: Array[Boolean] = Array.ofDim(size)

  /**
    * A condition as input bitsets: an equality between disjoint input sets (left and right) with
    * the distinct counts of its sides, or another condition over `refs`
    */
  private case class Edge(
      refs: Int,
      left: Int,
      right: Int,
      leftDistinct: Option[Double],
      rightDistinct: Option[Double]
  ):
    def isEqui: Boolean = left != 0

  private def mask(refs: Set[Int]): Int = refs.foldLeft(0)((m, i) => m | (1 << i))

  private def within(a: Int, s: Int): Boolean = (a & ~s) == 0

  /** Whether a condition over `refs` is applied at the join of s1 and s2 (their union is s) */
  private def placedAt(refs: Int, s: Int, s1: Int, s2: Int): Boolean =
    within(refs, s) && !within(refs, s1) && !within(refs, s2)

  /** Whether an equality is a key of a join of l and r: its left side over l, its right over r */
  private def isKey[C](sd: JoinEdge.Sides[C], l: Int, r: Int): Boolean =
    val a = mask(sd.leftRefs)
    val b = mask(sd.rightRefs)
    a != 0 && b != 0 && (a & b) == 0 && within(a, l) && within(b, r)

  private def analyze[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): IndexedSeq[Edge] =
    def distinct(side: C, refs: Set[Int]): Option[Double] =
      if refs.size == 1 then
        search.distinct(g.inputs(refs.head), side)
      else
        None
    g.edges
      .map { e =>
        val refs = mask(e.refs)
        e.sides match
          case Some(sd) =>
            val a = mask(sd.leftRefs)
            val b = mask(sd.rightRefs)
            if a != 0 && b != 0 && (a & b) == 0 then
              Edge(refs, a, b, distinct(sd.left, sd.leftRefs), distinct(sd.right, sd.rightRefs))
            else
              Edge(refs, 0, 0, None, None)
          case None =>
            Edge(refs, 0, 0, None, None)
      }
      .toIndexedSeq

  /** Whether an equality joins input sets s1 and s2 */
  private def connects(edges: IndexedSeq[Edge], s1: Int, s2: Int): Boolean = edges.exists { e =>
    e.isEqui &&
    ((within(e.left, s1) && within(e.right, s2)) || (within(e.left, s2) && within(e.right, s1)))
  }

end JoinEnumerator
