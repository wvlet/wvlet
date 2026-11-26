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
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * An aggregation filtered by an IN subquery that groups one of its tables by the join key reads
  * the subquery's groups in place of the table (`reuse_in_subquery_aggregates`), with the rows
  * DuckDB returns
  */
class EngineInSubqueryAggregatesTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.ReuseInSubqueryAggregates))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def relationOf(query: String): (Relation, Context) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    (GenSQL.expand(q, summon[Context]), summon[Context])

  /**
    * The query matches DuckDB with the rewrite on and off; whether the plan reads the subquery's
    * groups instead of computing the subquery at plan time
    */
  private def reusesGroups(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val (rel, ctx) = relationOf(query)
    given Context  = ctx
    val off        = conformance.compare(query, rel, target = disabled)
    off.matched shouldBe true
    // The subquery's condition over its groups, evaluated at run time, and lineitem read once
    def readsGroups(plan: String): Boolean =
      plan.contains("lit(200.00:decimal(38,2))") &&
        plan.linesIterator.count(_.contains("Scan[lineitem]")) == 1
    val offPlan = disabled.explain(rel)
    val onPlan  = conformance.engine.explain(rel)
    debug(onPlan)
    readsGroups(offPlan) shouldBe false
    readsGroups(onPlan)

  private val bigOrders =
    """({
      |  from lineitem
      |  group by l_orderkey
      |  where l_quantity.sum > 200
      |  select l_orderkey
      |})""".stripMargin

  test("reuse the IN subquery's sums for the outer aggregation (TPC-H q18)") {
    val query =
      s"""from customer, orders, lineitem
         |where o_orderkey in ${bigOrders}
         |  and c_custkey = o_custkey
         |  and o_orderkey = l_orderkey
         |group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
         |select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity.sum
         |order by o_totalprice desc, o_orderdate, o_orderkey
         |limit 100
         |""".stripMargin
    reusesGroups(query) shouldBe true
  }

  test("reuse the groups for sums, mins, and maxes, with the key as the IN target") {
    val query =
      s"""from orders, lineitem
         |where l_orderkey in ${bigOrders} and o_orderkey = l_orderkey
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.sum as q, l_extendedprice.max as hi,
         |  l_discount.min as lo
         |order by o_orderpriority
         |""".stripMargin
    reusesGroups(query) shouldBe true
  }

  test("reuse the groups when other rows meet a key more than once") {
    // Each customer row of a nation meets the orders of its nation's customers
    val query =
      s"""from nation, customer, orders, lineitem
         |where o_orderkey in ${bigOrders}
         |  and n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey
         |  and o_orderdate < '1996-01-01'::date
         |group by n_name
         |select n_name, l_quantity.sum as q
         |order by n_name
         |""".stripMargin
    reusesGroups(query) shouldBe true
  }

  test("reuse the groups of a subquery selecting its key under an alias, over JOIN ON") {
    val query =
      """from orders
        |join lineitem on o_orderkey = l_orderkey and o_orderstatus = 'O'
        |where o_orderkey in ({
        |  from lineitem group by l_orderkey where l_quantity.sum > 200 select l_orderkey as k
        |})
        |group by o_orderpriority
        |select o_orderpriority, l_quantity.sum as q
        |order by o_orderpriority
        |""".stripMargin
    reusesGroups(query) shouldBe true
  }

  test("plan the IN subquery as before when the groups can't be reused") {
    Seq(
      // No conjunct joins on the key, so nothing would reject the NULL key's group
      s"""from region, lineitem
         |where l_orderkey in ${bigOrders}
         |group by r_name
         |select r_name, l_quantity.sum as q
         |order by r_name""".stripMargin,
      // count(*) counts the table's rows
      s"""from orders, lineitem
         |where o_orderkey in ${bigOrders} and o_orderkey = l_orderkey
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.sum as q, count(*) as n
         |order by o_orderpriority""".stripMargin,
      // avg doesn't merge over groups
      s"""from orders, lineitem
         |where o_orderkey in ${bigOrders} and o_orderkey = l_orderkey
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.avg as q
         |order by o_orderpriority""".stripMargin,
      // A call over another table's column
      s"""from orders, lineitem
         |where o_orderkey in ${bigOrders} and o_orderkey = l_orderkey
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.sum as q, o_totalprice.sum as t
         |order by o_orderpriority""".stripMargin,
      // Another column of the table outside the calls
      s"""from orders, lineitem
         |where o_orderkey in ${bigOrders} and o_orderkey = l_orderkey and l_returnflag = 'R'
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.sum as q
         |order by o_orderpriority""".stripMargin,
      // The IN target isn't the join key
      s"""from orders, lineitem
         |where o_custkey in ${bigOrders} and o_orderkey = l_orderkey
         |group by o_orderpriority
         |select o_orderpriority, l_quantity.sum as q
         |order by o_orderpriority""".stripMargin
    ).foreach { query =>
      reusesGroups(query) shouldBe false
    }
  }

end EngineInSubqueryAggregatesTest
