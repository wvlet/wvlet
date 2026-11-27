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
  * ties. With `partition_probe_scan_cost`, a join whose build keys skip parts of a probe scan (each
  * holding one key value) is credited with the rows the scan no longer reads.
  */
class JoinEnumerator(options: OptimizerOptions):
  import JoinEnumerator.*

  private val probeScanCost     = options.isEnabled(Optimization.ProbeScanCost)
  private val joinProbeScanCost = probeScanCost && options.isEnabled(Optimization.JoinProbeScanCost)
  private val partitionCost     =
    options.isEnabled(Optimization.PartitionProbeScanCost) &&
      options.isEnabled(Optimization.KeySetRowGroups)

  /**
    * The cheapest join tree and the conditions left to apply over it (those reading no input), or
    * None when the graph is too large, needs a cross join, or exceeds [[PairBudget]]
    */
  def plan[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): Option[(P, List[C])] =
    planWithRows(g, search).map((tree, conds, _) => (tree, conds))

  /** [[plan]], with the estimated rows of the tree */
  def planWithRows[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K]
  ): Option[(P, List[C], Double)] =
    val n = g.inputs.size
    if n < 2 || n > MaxInputs then
      None
    else
      val (edges, groups) = keyGroups(g, search, analyze(g, search, partitionCost))
      val full            = (1 << n) - 1
      solve(g, edges, groups).map(dp =>
        (build(g, search, edges, dp, full), constants(g), dp.rows(full))
      )

  /** The best join of each input set, by bitset, or None if the full set has no plan */
  private def solve[P, C](
      g: JoinGraph[P, C],
      edges: IndexedSeq[Edge],
      groups: IndexedSeq[KeyGroup]
  ): Option[Table] =
    val n    = g.inputs.size
    val full = (1 << n) - 1
    val dp   = Table(1 << n)
    for i <- 0 until n do
      dp.rows(1 << i) = g.rows(i)
      dp.planned(1 << i) = true
    if partitionCost && edges.exists(e => e.leftPartition || e.rightPartition) then
      dp.readShares(n)
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
          if dp.planned(s1) && dp.planned(s2) && emit(dp, edges, groups, s1, s2) then
            pairs += 1
          sub = (sub - 1) & ext
      s += 1
    Option.when(pairs <= PairBudget && dp.planned(full))(dp)

  end solve

  /**
    * Cost the joins of s1 and s2 in both orders, keeping the cheapest plan of their union. Returns
    * false, costing nothing, when no equality joins them. The keys of a [[KeyGroup]] divide as
    * [[CardinalityEstimator.compositeDivisor]] estimates them
    */
  private def emit(
      dp: Table,
      edges: IndexedSeq[Edge],
      groups: IndexedSeq[KeyGroup],
      s1: Int,
      s2: Int
  ): Boolean =
    val s        = s1 | s2
    val d1       = IndexedSeq.newBuilder[Option[Double]]
    val d2       = IndexedSeq.newBuilder[Option[Double]]
    var divisors = List.empty[Double]
    var keys     = 0
    var filters  = 0
    // Groups whose edges are all keys here: every edge of a group joins the same two inputs
    var grouped = 0L
    if groups.nonEmpty then
      edges.foreach { e =>
        if e.group >= 0 then
          val placement = e.placement(s1, s2)
          if placement == Placement.Key || placement == Placement.SwappedKey then
            grouped |= 1L << e.group
      }
    var gi = 0
    while gi < groups.size do
      if (grouped & (1L << gi)) != 0 then
        val kg      = groups(gi)
        val aLeft   = within(1 << kg.a, s1)
        val members = kg.members.map(edges)
        // Distinct counts on the side of each input set, as the plain keys below take them
        def sideOf(e: Edge, first: Boolean): Option[Double] =
          if (e.leftInput == kg.a) == (aLeft == first) then
            e.leftDistinct
          else
            e.rightDistinct
        val g1       = members.map(sideOf(_, true))
        val g2       = members.map(sideOf(_, false))
        val (c1, c2) =
          if aLeft then
            (kg.aCoverage, kg.bCoverage)
          else
            (kg.bCoverage, kg.aCoverage)
        CardinalityEstimator.compositeDivisor(dp.rows(s1), g1, dp.rows(s2), g2, c1, c2) match
          case Some(d) =>
            divisors = d :: divisors
          case None =>
            // Unknown keys: plain pairs under the cap
            grouped &= ~(1L << gi)
      gi += 1
    // Every key's distinct counts and input (-1 for several) on each side, for the probe scan cost
    val all1 = IndexedSeq.newBuilder[Option[Double]]
    val all2 = IndexedSeq.newBuilder[Option[Double]]
    val ins1 = IndexedSeq.newBuilder[Int]
    val ins2 = IndexedSeq.newBuilder[Int]
    // Whether each key's side reads a partition key of its input, for the partition credit
    val part1    = IndexedSeq.newBuilder[Boolean]
    val part2    = IndexedSeq.newBuilder[Boolean]
    val partCost = dp.share != null
    edges.foreach { e =>
      val placement = e.placement(s1, s2)
      if probeScanCost || partCost then
        placement match
          case Placement.Key =>
            all1 += e.leftDistinct
            all2 += e.rightDistinct
            ins1 += e.leftInput
            ins2 += single(e.right)
            part1 += e.leftPartition
            part2 += e.rightPartition
          case Placement.SwappedKey =>
            all1 += e.rightDistinct
            all2 += e.leftDistinct
            ins1 += single(e.right)
            ins2 += e.leftInput
            part1 += e.rightPartition
            part2 += e.leftPartition
          case _ =>
      placement match
        case Placement.Key | Placement.SwappedKey
            if e.group >= 0 && (grouped & (1L << e.group)) != 0 =>
          keys += 1
        case Placement.Key =>
          keys += 1
          d1 += e.leftDistinct
          d2 += e.rightDistinct
        case Placement.SwappedKey =>
          keys += 1
          d1 += e.rightDistinct
          d2 += e.leftDistinct
        case Placement.Filter =>
          filters += 1
        case Placement.Below =>
    }
    if keys > 0 then
      var out = CardinalityEstimator.joinOutputRows(
        dp.rows(s1),
        d1.result(),
        dp.rows(s2),
        d2.result(),
        divisors
      )
      if filters > 0 then
        out = math.max(1.0, out * math.pow(CardinalityEstimator.DefaultSelectivity, filters))
      val base     = dp.cost(s1) + dp.cost(s2) + out
      lazy val ad1 = all1.result()
      lazy val ad2 = all2.result()
      lazy val in1 = ins1.result()
      lazy val in2 = ins2.result()
      lazy val pt1 = part1.result()
      lazy val pt2 = part2.result()
      // The read shares of the inputs of s for the plan being costed
      lazy val shares                           = Array.ofDim[Double](dp.inputs)
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
        val probe     = s & ~build
        val probeScan =
          val singleProbe = Integer.bitCount(probe) == 1
          if probeScanCost && (singleProbe || joinProbeScanCost) then
            val (probeDistinct, buildDistinct, probeInputs, buildInputs) =
              if probe == s1 then
                (ad1, ad2, in1, in2)
              else
                (ad2, ad1, in2, in1)
            // Inner joins never add key values: a build key has at most the rows of every join
            // of the build's tree holding its input (TPC-DS q72's item key, not the rows that
            // joining date_dim by week multiplied)
            val capped = buildDistinct
              .indices
              .map(k => buildDistinct(k).map(d => math.min(d, pathRows(dp, build, buildInputs(k)))))
            // A probe that is a join holds at most its joins' rows of each key, likewise
            val probeCapped =
              if singleProbe then
                probeDistinct
              else
                probeDistinct
                  .indices
                  .map(k =>
                    probeDistinct(k).map(d => math.min(d, pathRows(dp, probe, probeInputs(k))))
                  )
            dp.rows(probe) * passRate(probeCapped, capped, buildRows)
          else
            0.0
          end if
        end probeScan
        val partitionCredit =
          if partCost then
            val (probeDistinct, buildDistinct, probeInputs, buildInputs, probeParts) =
              if probe == s1 then
                (ad1, ad2, in1, in2, pt1)
              else
                (ad2, ad1, in2, in1, pt2)
            partitionReads(
              dp,
              s1,
              s2,
              probe,
              build,
              probeDistinct,
              buildDistinct,
              probeInputs,
              buildInputs,
              probeParts,
              shares
            )
          else
            0.0
        val cost = base + buildRows + pipelineBreak + probeScan + partitionCredit
        if !dp.planned(s) || cost < dp.cost(s) then
          dp.planned(s) = true
          dp.cost(s) = cost
          if partCost then
            System.arraycopy(shares, 0, dp.share, s * dp.inputs, dp.inputs)
          dp.rows(s) = out
          dp.left(s) = left
          dp.right(s) = right
          dp.buildLeft(s) = buildLeft
      end consider
      consider(s1, s2)
      consider(s2, s1)
    end if
    keys > 0

  end emit

  /**
    * The change in rows read by the probe scans of a join of s1 and s2 building on `build`
    * (`partition_probe_scan_cost`; at most 0.0), with the read shares of the inputs of the union
    * written to `shares`: a key whose probe side reads a partition key of one input on the probe
    * pipeline of `probe` (where the build's join filter reaches the scan) keeps the build keys'
    * share of its values ([[passRate]] of that key alone) of the input's rows, and an input keeps
    * the smallest share of its keys
    */
  private def partitionReads(
      dp: Table,
      s1: Int,
      s2: Int,
      probe: Int,
      build: Int,
      probeDistinct: IndexedSeq[Option[Double]],
      buildDistinct: IndexedSeq[Option[Double]],
      probeInputs: IndexedSeq[Int],
      buildInputs: IndexedSeq[Int],
      probeParts: IndexedSeq[Boolean],
      shares: Array[Double]
  ): Double =
    val n = dp.inputs
    var i = 0
    while i < n do
      val from =
        if within(1 << i, s1) then
          s1
        else
          s2
      shares(i) =
        if within(1 << i, s1 | s2) then
          dp.share(from * n + i)
        else
          1.0
      i += 1
    var credit = 0.0
    var k      = 0
    while k < probeDistinct.size do
      val in = probeInputs(k)
      if probeParts(k) && in >= 0 && onProbePipeline(dp, probe, in) then
        for
          p <- probeDistinct(k)
          b <- buildDistinct(k)
        do
          val keys  = math.min(b, pathRows(dp, build, buildInputs(k)))
          val share = math.min(1.0, math.min(keys, dp.rows(build)) / math.max(p, 1.0))
          if share < shares(in) then
            credit += dp.rows(1 << in) * (share - shares(in))
            shares(in) = share
      k += 1
    credit

  end partitionReads

  /**
    * Whether input i is read by the probe pipeline of the best tree of input set s: s itself, or on
    * the probe side of each join down to it, so that the join filters of builds over s reach it
    */
  private def onProbePipeline(dp: Table, s: Int, i: Int): Boolean =
    var t  = s
    var ok = within(1 << i, s)
    while ok && Integer.bitCount(t) > 1 do
      t =
        if dp.buildLeft(t) then
          dp.right(t)
        else
          dp.left(t)
      ok = within(1 << i, t)
    ok

  /**
    * The fewest rows of the joins in the best tree of input set s that hold input i, down to i
    * itself (the rows of s when i is -1, a key over several inputs)
    */
  private def pathRows(dp: Table, s: Int, i: Int): Double =
    var rows = dp.rows(s)
    if i >= 0 then
      var t = s
      while Integer.bitCount(t) > 1 do
        t =
          if within(1 << i, dp.left(t)) then
            dp.left(t)
          else
            dp.right(t)
        rows = math.min(rows, dp.rows(t))
    rows

  /**
    * The fraction of a probe input's rows that the runtime join filters of a build of `buildRows`
    * rows are expected to keep: for each key, the build's distinct values (at most its rows) over
    * the probe's, and the smallest over the keys. 1.0 when no key has known distinct counts on both
    * sides
    */
  private[plan] def passRate(
      probeDistinct: IndexedSeq[Option[Double]],
      buildDistinct: IndexedSeq[Option[Double]],
      buildRows: Double
  ): Double = probeDistinct
    .indices
    .flatMap { k =>
      for
        p <- probeDistinct(k)
        b <- buildDistinct(k)
      yield math.min(1.0, math.min(b, buildRows) / math.max(p, 1.0))
    }
    .minOption
    .getOrElse(1.0)

  /** The plan of the best join tree of input set s */
  private def build[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K],
      edges: IndexedSeq[Edge],
      dp: Table,
      s: Int
  ): P =
    if Integer.bitCount(s) == 1 then
      g.inputs(Integer.numberOfTrailingZeros(s))
    else
      val l       = dp.left(s)
      val r       = dp.right(s)
      val left    = build(g, search, edges, dp, l)
      val right   = build(g, search, edges, dp, r)
      val pairs   = ListBuffer.empty[(C, C)]
      val filters = ListBuffer.empty[C]
      // The placements the join was costed with
      g.edges
        .zip(edges)
        .foreach { (je, e) =>
          e.placement(l, r) match
            case Placement.Key =>
              pairs += ((je.sides.get.left, je.sides.get.right))
            case Placement.SwappedKey =>
              pairs += ((je.sides.get.right, je.sides.get.left))
            case Placement.Filter =>
              filters += je.cond
            case Placement.Below =>
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

    /** Inputs of the graph, with [[share]] */
    var inputs: Int = 0

    /**
      * The share of each input's rows that the scans of the best tree of each set read
      * (`partition_probe_scan_cost`), by set * [[inputs]] + input; null when no key is a partition
      * key
      */
    var share: Array[Double] = null

    /** Allocate [[share]] for n inputs, every input read whole */
    def readShares(n: Int): Unit =
      inputs = n
      share = Array.fill(size * n)(1.0)

  /**
    * A condition as input bitsets: an equality between disjoint input sets (left and right) with
    * the distinct counts of its sides, or another condition over `refs`
    */
  private case class Edge(
      refs: Int,
      left: Int,
      right: Int,
      leftDistinct: Option[Double],
      rightDistinct: Option[Double],
      leftInput: Int = -1,
      group: Int = -1,
      leftPartition: Boolean = false,
      rightPartition: Boolean = false
  ):
    private def isEqui: Boolean = left != 0

    /** Whether this is an equality between disjoint input sets */
    def isEquiPair: Boolean = isEqui

    /** Where a join of s1 and s2 applies this condition */
    def placement(s1: Int, s2: Int): Placement =
      if isEqui && within(left, s1) && within(right, s2) then
        Placement.Key
      else if isEqui && within(left, s2) && within(right, s1) then
        Placement.SwappedKey
      else if refs != 0 && within(refs, s1 | s2) && !within(refs, s1) && !within(refs, s2) then
        Placement.Filter
      else
        // Applied below the join, above it, or (reading no input) over the whole tree
        Placement.Below

  /** How a join applies a condition */
  private enum Placement:
    /** A hash key, the left side over the join's left input */
    case Key

    /** A hash key, the right side over the join's left input */
    case SwappedKey

    /** A filter over the join, as it reads both inputs */
    case Filter

    /** Not at this join */
    case Below

  private def mask(refs: Set[Int]): Int = refs.foldLeft(0)((m, i) => m | (1 << i))

  private def within(a: Int, s: Int): Boolean = (a & ~s) == 0

  /**
    * Equalities between the same two inputs (`a` and another), when there are several: a key of
    * several columns, with what the inputs' declared unique keys say about each side
    */
  private case class KeyGroup(
      a: Int,
      members: IndexedSeq[Int],
      aCoverage: KeyCoverage,
      bCoverage: KeyCoverage
  )

  /** Single input of a side, or -1 */
  private def single(m: Int): Int =
    if m != 0 && Integer.bitCount(m) == 1 then
      Integer.numberOfTrailingZeros(m)
    else
      -1

  /**
    * The [[KeyGroup]]s of the graph whose inputs declare keys, at most 64 (one bit each in `emit`),
    * with the edges numbered by their group
    */
  private def keyGroups[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K],
      edges: IndexedSeq[Edge]
  ): (IndexedSeq[Edge], IndexedSeq[KeyGroup]) =
    val pairs = edges
      .indices
      .filter { i =>
        val e = edges(i)
        e.isEquiPair && single(e.left) >= 0 && single(e.right) >= 0
      }
      .groupBy { i =>
        val (a, b) = (single(edges(i).left), single(edges(i).right))
        (math.min(a, b), math.max(a, b))
      }
      .toIndexedSeq
      .filter(_._2.size > 1)
      .sortBy(_._1)
      .take(64)
    lazy val graphEdges = g.edges.toIndexedSeq
    val groups          = IndexedSeq.newBuilder[KeyGroup]
    pairs.foreach { case ((a, b), members) =>
      val jes                     = members.map(graphEdges)
      def sidesAt(i: Int): Seq[C] = members
        .zip(jes)
        .map { (m, je) =>
          if edges(m).leftInput == i then
            je.sides.get.left
          else
            je.sides.get.right
        }
      val ca = search.keyCoverage(g.inputs(a), sidesAt(a))
      val cb = search.keyCoverage(g.inputs(b), sidesAt(b))
      if ca != KeyCoverage.Unknown || cb != KeyCoverage.Unknown then
        groups += KeyGroup(a, members, ca, cb)
    }
    val result   = groups.result()
    val numbered = edges.toArray
    result
      .zipWithIndex
      .foreach { (kg, gi) =>
        kg.members.foreach(m => numbered(m) = numbered(m).copy(group = gi))
      }
    (numbered.toIndexedSeq, result)

  end keyGroups

  /**
    * The edges of g with their sides' distinct counts and, with `partitions`, whether each side
    * reads a partition key of its input ([[JoinSearch.partitionKey]])
    */
  private def analyze[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K],
      partitions: Boolean
  ): IndexedSeq[Edge] =
    def distinct(side: C, refs: Set[Int]): Option[Double] =
      if refs.size == 1 then
        search.distinct(g.inputs(refs.head), side)
      else
        None
    def partition(side: C, refs: Set[Int]): Boolean =
      partitions && refs.size == 1 && search.partitionKey(g.inputs(refs.head), side)
    g.edges
      .map { e =>
        val refs = mask(e.refs)
        e.sides match
          case Some(sd) =>
            val a = mask(sd.leftRefs)
            val b = mask(sd.rightRefs)
            if a != 0 && b != 0 && (a & b) == 0 then
              Edge(
                refs,
                a,
                b,
                distinct(sd.left, sd.leftRefs),
                distinct(sd.right, sd.rightRefs),
                leftInput = single(a),
                leftPartition = partition(sd.left, sd.leftRefs),
                rightPartition = partition(sd.right, sd.rightRefs)
              )
            else
              Edge(refs, 0, 0, None, None)
          case None =>
            Edge(refs, 0, 0, None, None)
      }
      .toIndexedSeq

  end analyze

end JoinEnumerator
