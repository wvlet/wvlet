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
package wvlet.lang.engine

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * Aggregations over an inner join whose calls read one side aggregate that side first
  * (`eager_aggregation`), with the rows DuckDB returns
  */
class EngineEagerAggregationTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.EagerAggregation))

  // The TPC-H tables with their declared primary keys
  private val keyed = EngineConformance.tpch(options, declaredKeys = true)

  override def afterAll: Unit =
    conformance.close()
    keyed.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def explain(e: WvletEngine, query: String): String =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val plan      = e.explain(GenSQL.expand(q, summon[Context]))
    debug(plan)
    plan

  /** The query matches DuckDB with the rewrite on and off, and returns the plans on and off */
  private def check(
      query: String,
      disabled: WvletEngine = disabled,
      conformance: EngineConformance = conformance
  ): (String, String) =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    (explain(conformance.engine, query), explain(disabled, query))

  private def aggregations(plan: String): Int = plan.linesIterator.count(_.contains("HashAgg("))

  test("aggregate the orders of each customer before joining the customer strings") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |group by c_name, c_address
        |select c_name, c_address, sum(o_totalprice) as s, count(*) as n, count(o_comment) as c,
        |  min(o_orderdate) as first, max(o_orderdate) as last
        |order by c_name
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off) + 1
  }

  test("combine the partial results for every duplicate of a join key") {
    // partsupp has four rows per part: each partial sum of lineitem joins all four
    val (on, off) = check("""from lineitem, partsupp
        |where l_partkey = ps_partkey
        |group by ps_partkey, ps_suppkey
        |select ps_partkey, ps_suppkey, sum(l_quantity) as q, count(*) as n, max(l_shipdate) as last
        |order by ps_partkey, ps_suppkey
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off) + 1
  }

  test("group the aggregated side by the columns a condition over the join reads") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey and o_custkey % 7 != c_nationkey
        |group by c_name
        |select c_name, sum(o_totalprice) as s, count(*) as n
        |order by c_name
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off) + 1
  }

  test("not aggregate into groups much finer than the outer ones") {
    // 1,500 partial groups (customers) for 5 outer groups (market segments)
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |group by c_mktsegment
        |select c_mktsegment, sum(o_totalprice) as s
        |order by c_mktsegment
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off)
  }

  test("not aggregate calls that could fail on rows the join drops") {
    // A division by a column could fail on an order without a customer, which the join drops
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |group by c_name
        |select c_name, sum(o_totalprice / o_custkey) as s
        |order by c_name
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off)
  }

  test("not aggregate before a join with a selective side") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey and c_mktsegment = 'BUILDING'
        |group by c_name
        |select c_name, sum(o_totalprice) as s
        |order by c_name
        |""".stripMargin)
    aggregations(on) shouldBe aggregations(off)
  }

  test("aggregate the finest set of a rollup over a join before the join") {
    // The finest set's aggregation is a shared input of the coarser sets, which the runtime
    // passes over the query don't reach
    val noGroupingSets = conformance
      .engine
      .withOptimizer(options.withDisabled(Optimization.EagerGroupingSets))
    val (on, off) = check(
      """from lineitem, part
        |where l_partkey = p_partkey
        |group by rollup(p_brand, p_type)
        |select p_brand, p_type, sum(l_quantity) as q, count(*) as n, avg(l_extendedprice) as p
        |order by p_brand nulls first, p_type nulls first
        |""".stripMargin,
      noGroupingSets
    )
    aggregations(on) shouldBe aggregations(off) + 1
  }

  private val noOtherSide = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.EagerOtherSideMinMax))

  test("keep mins and maxes of the other side's columns above the join") {
    val (on, off) = check(
      """from orders, customer
        |where o_custkey = c_custkey
        |group by c_name
        |select c_name, sum(o_totalprice) as s, max(c_acctbal) as b, min(c_nationkey) as n,
        |  count(*) as c
        |order by c_name
        |""".stripMargin,
      noOtherSide
    )
    aggregations(on) shouldBe aggregations(off) + 1
  }

  test("aggregate before a join with a side crossed with a scalar") {
    // TPC-DS q23's best customers: each customer's sum against the maximum of the sums
    val (on, off) = check(
      """with mx as {
        |  from {
        |    from orders
        |    group by o_custkey
        |    select o_custkey, sum(o_totalprice) as s
        |  }
        |  select max(s) as m
        |}
        |from orders, customer, mx
        |where o_custkey = c_custkey
        |group by c_custkey
        |where sum(o_totalprice) > 0.5 * max(m)
        |select c_custkey, sum(o_totalprice) as s
        |order by c_custkey
        |""".stripMargin,
      noOtherSide
    )
    aggregations(on) shouldBe aggregations(off) + 1
  }

  private val noJoinTrees = keyed
    .engine
    .withOptimizer(options.withDisabled(Optimization.EagerAggregationInJoinTrees))

  /** The plan lines of hash aggregations whose output starts with column `col` */
  private def aggregationsBy(plan: String, col: String): Int = plan
    .linesIterator
    .count(l => l.contains("HashAgg(") && l.contains(s"-> [${col}:"))

  test("sum order lines per customer key before looking up the customer and nation") {
    // TPC-H q10: every outer group is one customer row, so the pre-aggregation replaces it
    val (on, off) = check(
      """from lineitem, orders, customer, nation
        |where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey
        |group by c_custkey, c_name, n_name
        |select c_custkey, c_name, n_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
        |  count(*) as n, max(l_shipdate) as last
        |order by revenue desc, c_custkey
        |limit 20
        |""".stripMargin,
      noJoinTrees,
      keyed
    )
    aggregationsBy(on, "o_custkey") shouldBe 1
    aggregationsBy(off, "o_custkey") shouldBe 0
    aggregations(on) shouldBe aggregations(off)
  }

  test("not aggregate in a join tree when outer groups may span several partial groups") {
    // c_name isn't a declared key: an outer group may hold several customers' partial sums
    val (on, off) = check(
      """from lineitem, orders, customer, nation
        |where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey
        |group by c_name, n_name
        |select c_name, n_name, sum(l_quantity) as q, count(*) as n
        |order by c_name
        |""".stripMargin,
      noJoinTrees,
      keyed
    )
    on shouldBe off
  }

  test("not aggregate below a selective join in a join tree") {
    val (on, off) = check(
      """from lineitem, orders, customer, nation
        |where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey
        |  and c_mktsegment = 'BUILDING'
        |group by c_custkey, c_name, n_name
        |select c_custkey, c_name, n_name, sum(l_quantity) as q
        |order by c_custkey
        |""".stripMargin,
      noJoinTrees,
      keyed
    )
    on shouldBe off
  }

end EngineEagerAggregationTest
