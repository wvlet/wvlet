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

import wvlet.uni.test.UniTest

import scala.util.Random

class JoinEnumeratorTest extends UniTest:
  import JoinEnumeratorTest.*

  private val options = OptimizerOptions()

  private def exact(g: JoinGraph[Tree, String]): Option[(Tree, List[String])] = JoinEnumerator(
    options
  ).plan(g, TreeSearch)

  private def edge(id: Int, l: Set[Int], r: Set[Int]): JoinEdge[String] = JoinEdge(
    s"e${id}",
    l ++ r,
    Some(JoinEdge.Sides(s"l${id}", l, s"r${id}", r))
  )

  private def filterEdge(id: Int, refs: Set[Int]): JoinEdge[String] = JoinEdge(
    s"f${id}",
    refs,
    None
  )

  /** A random graph: a random spanning tree of equalities, then extra conditions */
  private def randomGraph(rnd: Random, n: Int): JoinGraph[Tree, String] =
    var id            = 0
    val edges         = List.newBuilder[JoinEdge[String]]
    def nextId(): Int =
      id += 1
      id
    for i <- 1 until n do
      edges += edge(nextId(), Set(rnd.nextInt(i)), Set(i))
    for _ <- 0 until rnd.nextInt(n + 1) do
      val refs = rnd.shuffle((0 until n).toList).take(2 + rnd.nextInt(math.min(3, n - 1)))
      rnd.nextInt(3) match
        case 0 =>
          // A hyperedge: an equality whose side reads several inputs
          val k = 1 + rnd.nextInt(refs.size - 1)
          edges += edge(nextId(), refs.take(k).toSet, refs.drop(k).toSet)
        case 1 =>
          edges += edge(nextId(), Set(refs(0)), Set(refs(1)))
        case _ =>
          edges += filterEdge(nextId(), refs.toSet)
    if rnd.nextInt(4) == 0 then
      edges += filterEdge(nextId(), Set.empty)
    JoinGraph(
      (0 until n).map(i => Leaf(i): Tree),
      (0 until n).map(_ => math.pow(10, 1 + rnd.nextInt(6)).toDouble),
      edges.result()
    )

  test("should join every input once and place every condition once") {
    val rnd = Random(42)
    for _ <- 0 until 300 do
      val n = 2 + rnd.nextInt(JoinEnumerator.MaxInputs - 1)
      val g = randomGraph(rnd, n)
      exact(g) match
        case Some((tree, remaining)) =>
          leaves(tree).sorted shouldBe (0 until n).toList
          val placed = conds(tree) ++ remaining
          placed.sorted shouldBe g.edges.map(_.cond).sorted
          remaining shouldBe g.edges.filter(_.refs.isEmpty).map(_.cond)
          // Every key and filter is applied where all the inputs it reads are joined
          validate(tree, g)
        case None =>
          // The spanning tree of equalities connects every input
          fail(s"No plan for ${g}")
  }

  test("should build a bushy tree for a snowflake under a large fact table") {
    // sales(0) - customer(1) - address(2): address is keyed by customer, so joining them first
    // probes the sales rows once
    val g = JoinGraph(
      IndexedSeq[Tree](Leaf(0), Leaf(1), Leaf(2)),
      IndexedSeq(1e7, 1e5, 1e5),
      List(edge(1, Set(0), Set(1)), edge(2, Set(1), Set(2)))
    )
    val (tree, remaining) = exact(g).get
    remaining shouldBe Nil
    tree match
      case Join(Leaf(0), right: Join, _, false) =>
        leaves(right).sorted shouldBe List(1, 2)
      case other =>
        fail(s"Expected sales to probe a join of customer and address: ${other}")
  }

  test("should keep a left-deep probe chain over a star") {
    val g = JoinGraph(
      (0 to 3).map(i => Leaf(i): Tree),
      IndexedSeq(1e7, 1e3, 1e3, 1e3),
      List(edge(1, Set(0), Set(1)), edge(2, Set(0), Set(2)), edge(3, Set(0), Set(3)))
    )
    val (tree, _)                    = exact(g).get
    def probeChain(t: Tree): Boolean =
      t match
        case Leaf(0) =>
          true
        case Join(l, _: Leaf, _, false) =>
          probeChain(l)
        case Filtered(t, _) =>
          probeChain(t)
        case _ =>
          false
    probeChain(tree) shouldBe true
  }

  test("should leave graphs needing a cross join to the greedy search") {
    val g = JoinGraph(
      IndexedSeq[Tree](Leaf(0), Leaf(1)),
      IndexedSeq(10.0, 10.0),
      List(filterEdge(1, Set(0, 1)))
    )
    exact(g) shouldBe None
  }

  test("should leave graphs over the input limit to the greedy search") {
    val n = JoinEnumerator.MaxInputs + 1
    val g = JoinGraph(
      (0 until n).map(i => Leaf(i): Tree),
      (0 until n).map(_ => 100.0),
      (1 until n).map(i => edge(i, Set(i - 1), Set(i))).toList
    )
    exact(g) shouldBe None
    // A chain at the limit is enumerated
    val m = JoinEnumerator.MaxInputs
    exact(g.copy(inputs = g.inputs.take(m), rows = g.rows.take(m), edges = g.edges.take(m - 1)))
      .isDefined shouldBe true
  }

  test("should leave graphs over the pair budget to the greedy search") {
    // A clique of 10 inputs has about 28,000 csg-cmp pairs
    val n     = JoinEnumerator.MaxInputs
    var id    = 0
    val edges =
      for
        i <- 0 until n
        j <- i + 1 until n
      yield
        id += 1
        edge(id, Set(i), Set(j))
    val g = JoinGraph(
      (0 until n).map(i => Leaf(i): Tree),
      (0 until n).map(_ => 100.0),
      edges.toList
    )
    exact(g) shouldBe None
  }

  test("should plan through JoinOrderOptimizer with exact_join_order on or off") {
    val rnd = Random(7)
    val g   = randomGraph(rnd, 6)
    val on  = JoinOrderOptimizer(options).plan(g, TreeSearch)
    val off = JoinOrderOptimizer(options.withDisabled(Optimization.ExactJoinOrder)).plan(
      g,
      TreeSearch
    )
    leaves(on._1).sorted shouldBe (0 until 6).toList
    leaves(off._1).sorted shouldBe (0 until 6).toList
  }

  test("should estimate a probe's rows from the distinct keys of its build") {
    val e = JoinEnumerator(options)
    // 214 build dates of 1,839 probe dates; the build's rows cap its distinct values
    e.passRate(IndexedSeq(Some(1839.0)), IndexedSeq(Some(73049.0)), 214) shouldBe 214.0 / 1839
    // The most selective key, and 1.0 without distinct counts
    e.passRate(
      IndexedSeq(Some(100.0), Some(10.0)),
      IndexedSeq(Some(50.0), Some(10.0)),
      1e6
    ) shouldBe 0.5
    e.passRate(IndexedSeq(None), IndexedSeq(Some(5.0)), 10) shouldBe 1.0
  }

  test("should filter a fact table by its date before joining it to another fact table") {
    // TPC-DS q25: catalog_sales(0) ⋈ date d3(1) and store_returns(2) ⋈ date d2(3), with
    // catalog_sales ⋈ store_returns on item (e5) and customer (e6), no key of either table
    val rows       = IndexedSeq(1.44e7, 214.0, 2.9e6, 214.0)
    val distinctOf = Map(
      "l1" -> 1839.0,
      "r1" -> 214.0,
      "l2" -> 2000.0,
      "r2" -> 214.0,
      "l5" -> 1.02e5,
      "r5" -> 1.02e5,
      "l6" -> 5e5,
      "r6" -> 5e5
    )
    val search =
      new JoinSearch[Tree, String, List[(String, String)]]:
        export TreeSearch.{keys, outputRows, join, cross, filter}
        override def distinct(input: Tree, side: String): Option[Double] = distinctOf.get(side)
        override def keyCoverage(input: Tree, sides: Seq[String]): KeyCoverage =
          KeyCoverage.NotUnique
    val g = JoinGraph(
      (0 to 3).map(i => Leaf(i): Tree),
      rows,
      List(
        edge(1, Set(0), Set(1)),
        edge(2, Set(2), Set(3)),
        edge(5, Set(0), Set(2)),
        edge(6, Set(0), Set(2))
      )
    )
    def subtrees(t: Tree): List[Tree] =
      t match
        case j @ Join(l, r, _, _) =>
          j :: subtrees(l) ++ subtrees(r)
        case Filtered(c, _) =>
          subtrees(c)
        case leaf =>
          List(leaf)
    def datedSales(t: Tree): Boolean = subtrees(t).exists(s => leaves(s).sorted == List(0, 1))
    datedSales(JoinEnumerator(options).plan(g, search).get._1) shouldBe true
    // Charging no probe scan, catalog_sales joins store_returns first: the estimate is tiny
    datedSales(
      JoinEnumerator(options.withDisabled(Optimization.ProbeScanCost)).plan(g, search).get._1
    ) shouldBe false
  }

  test("should probe a large input with a join rather than build on it") {
    // TPC-H q10 at SF10: customer(0) ⋈ filtered orders(1) on custkey (e1), orders ⋈ returned
    // lineitem(2) on orderkey (e2), customer ⋈ nation(3) (e3)
    val rows       = IndexedSeq(1.5e6, 573566.0, 1.476e7, 25.0)
    val distinctOf = Map(
      "l1" -> 1.5e6,
      "r1" -> 573566.0,
      "l2" -> 573566.0,
      "r2" -> 1.5e7,
      "l3" -> 25.0,
      "r3" -> 25.0
    )
    val search =
      new JoinSearch[Tree, String, List[(String, String)]]:
        export TreeSearch.{keys, outputRows, join, cross, filter}
        override def distinct(input: Tree, side: String): Option[Double] = distinctOf.get(side)
        override def keyCoverage(input: Tree, sides: Seq[String]): KeyCoverage =
          KeyCoverage.NotUnique
    val g = JoinGraph(
      (0 to 3).map(i => Leaf(i): Tree),
      rows,
      List(edge(1, Set(0), Set(1)), edge(2, Set(1), Set(2)), edge(3, Set(0), Set(3)))
    )
    def customerBuild(t: Tree): Option[List[Int]] =
      t match
        case Join(l, r, _, buildLeft) if leaves(t).sorted == List(0, 1, 2) =>
          Some(
            leaves(
              if buildLeft then
                l
              else
                r
            ).sorted
          )
        case Join(l, r, _, _) =>
          customerBuild(l).orElse(customerBuild(r))
        case Filtered(c, _) =>
          customerBuild(c)
        case _ =>
          None
    // Customer probes the order lines' build, as its runtime filter keeps 38% of it
    customerBuild(JoinEnumerator(options).plan(g, search).get._1) shouldBe Some(List(1, 2))
    // Charging single-input probes alone, customer becomes the build to avoid its probe charge
    customerBuild(
      JoinEnumerator(options.withDisabled(Optimization.JoinProbeScanCost)).plan(g, search).get._1
    ) shouldBe Some(List(0))
  }

end JoinEnumeratorTest

object JoinEnumeratorTest:
  sealed trait Tree
  case class Leaf(i: Int) extends Tree
  case class Join(left: Tree, right: Tree, keys: List[(String, String)], buildLeft: Boolean)
      extends Tree

  case class Filtered(child: Tree, conds: List[String]) extends Tree

  def leaves(t: Tree): List[Int] =
    t match
      case Leaf(i) =>
        List(i)
      case Join(l, r, _, _) =>
        leaves(l) ++ leaves(r)
      case Filtered(c, _) =>
        leaves(c)

  /** Conditions placed in a tree: hash join keys (`e<id>` of sides `l<id>`, `r<id>`) and filters */
  def conds(t: Tree): List[String] =
    t match
      case Leaf(_) =>
        Nil
      case Join(l, r, keys, _) =>
        conds(l) ++ conds(r) ++ keys.map((a, _) => s"e${a.drop(1)}")
      case Filtered(c, cs) =>
        conds(c) ++ cs

  /** Checks that every key side reads only its join side, and every filter only joined inputs */
  def validate(t: Tree, g: JoinGraph[Tree, String]): Unit =
    def refsOf(side: String): Set[Int] =
      val e = g.edges.find(_.cond == s"e${side.drop(1)}").get
      if side.startsWith("l") then
        e.sides.get.leftRefs
      else
        e.sides.get.rightRefs
    t match
      case Leaf(_)             =>
      case Join(l, r, keys, _) =>
        keys.foreach { (a, b) =>
          assert(refsOf(a).subsetOf(leaves(l).toSet), s"${a} of ${t}")
          assert(refsOf(b).subsetOf(leaves(r).toSet), s"${b} of ${t}")
        }
        validate(l, g)
        validate(r, g)
      case Filtered(c, cs) =>
        cs.foreach { cond =>
          val e = g.edges.find(_.cond == cond).get
          assert(e.refs.subsetOf(leaves(c).toSet), s"${cond} of ${t}")
        }
        validate(c, g)

  /** Join trees of [[Tree]], with key pairs of side names; rows are estimated as unknown keys */
  object TreeSearch extends JoinSearch[Tree, String, List[(String, String)]]:
    override def keys(
        current: Tree,
        next: Tree,
        pairs: Seq[(String, String)]
    ): List[(String, String)] = pairs.toList

    override def distinct(input: Tree, side: String): Option[Double] = None

    override def outputRows(
        current: Tree,
        currentRows: Double,
        next: Tree,
        nextRows: Double,
        keys: List[(String, String)]
    ): Double = math.max(currentRows, nextRows)

    override def join(
        current: Tree,
        next: Tree,
        keys: List[(String, String)],
        buildLeft: Boolean
    ): Tree = Join(current, next, keys, buildLeft)

    override def cross(current: Tree, next: Tree): Tree     = Join(current, next, Nil, false)
    override def filter(p: Tree, conds: List[String]): Tree = Filtered(p, conds)

end JoinEnumeratorTest
