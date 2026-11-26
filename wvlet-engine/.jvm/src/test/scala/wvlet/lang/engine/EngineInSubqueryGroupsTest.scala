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
  * An aggregation filtered by `key in (subquery)` whose subquery aggregates the same input computes
  * its groups once, which the subquery merges (`share_in_subquery_groups`), with the rows DuckDB
  * returns
  */
class EngineInSubqueryGroupsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.ShareInSubqueryGroups))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def explain(e: WvletEngine, query: String): String =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val plan      = e.explain(GenSQL.expand(q, summon[Context]))
    debug(plan)
    plan

  /**
    * The query matches DuckDB with the rewrite on and off; whether the rewrite filtered groups
    * computed at plan time
    */
  private def filtersGroups(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    // The groups, computed at plan time, then filtered by the subquery's values
    def isGroups(plan: String): Boolean = plan
      .linesIterator
      .map(_.trim)
      .sliding(2)
      .exists(l => l.size == 2 && l(0).startsWith("Values") && l(1).startsWith("↓ Filter(in("))
    isGroups(explain(disabled, query)) shouldBe false
    isGroups(explain(conformance.engine, query))

  // The subquery lists the tables in another order, under an alias
  private def topNations(n: Int, value: String = "s_acctbal.sum"): String =
    s"""({
       |  from nation as n, supplier
       |  where s_nationkey = n.n_nationkey
       |  group by n_name
       |  select n_name, rank() over (order by ${value} desc) as ranking
       |  where ranking <= ${n}
       |  select n_name
       |})""".stripMargin

  test("filter the groups by an IN subquery over the same join (TPC-DS q70)") {
    val query =
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and n_name in ${topNations(5)}
         |group by n_name, n_regionkey
         |select n_name, n_regionkey, s_acctbal.sum as total, s_acctbal.min as lo, s_acctbal.max as hi
         |order by n_name
         |""".stripMargin
    filtersGroups(query) shouldBe true
  }

  test("filter the finest groups of a rollup") {
    val query =
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and n_name in ${topNations(3)}
         |group by rollup(n_regionkey, n_name)
         |select n_regionkey, n_name, s_acctbal.sum as total,
         |  grouping(n_regionkey) + grouping(n_name) as level
         |order by level desc, n_regionkey, n_name
         |""".stripMargin
    filtersGroups(query) shouldBe true
  }

  test("filter the groups when the subquery's calls are not the aggregation's") {
    // The subquery reads the input on its own, then the groups are filtered as before
    val query =
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and n_name in ${topNations(4, "s_suppkey.count")}
         |group by n_name
         |select n_name, s_acctbal.sum as total
         |order by n_name
         |""".stripMargin
    filtersGroups(query) shouldBe true
  }

  test("filter by a subquery with NULL values and no matches") {
    Seq(
      "({ from nation, supplier where s_nationkey = n_nationkey group by n_name select if s_acctbal.sum > 0 then null else n_name })",
      "({ from nation, supplier where s_nationkey = n_nationkey group by n_name where s_acctbal.sum < 0 select n_name })"
    ).foreach { sub =>
      val query =
        s"""from supplier, nation
           |where s_nationkey = n_nationkey and n_name in ${sub}
           |group by n_name
           |select n_name, s_acctbal.sum as total
           |order by n_name
           |""".stripMargin
      filtersGroups(query) shouldBe true
    }
  }

  test("plan the IN filter as before when the groups can't be shared") {
    Seq(
      // The IN target is no grouping key
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and n_name in ${topNations(5)}
         |group by n_regionkey
         |select n_regionkey, s_acctbal.sum as total
         |order by n_regionkey""".stripMargin,
      // avg doesn't merge over groups
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and n_name in ${topNations(5)}
         |group by n_name
         |select n_name, s_acctbal.avg as a
         |order by n_name""".stripMargin,
      // The subquery aggregates another input
      s"""from supplier, nation
         |where s_nationkey = n_nationkey and s_acctbal > 0 and n_name in ${topNations(5)}
         |group by n_name
         |select n_name, s_acctbal.sum as total
         |order by n_name""".stripMargin
    ).foreach { query =>
      filtersGroups(query) shouldBe false
    }
  }

end EngineInSubqueryGroupsTest
