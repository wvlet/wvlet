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
  * A scalar subquery in an aggregation's condition that aggregates the aggregation's own input is
  * totaled over the computed groups (`having_totals_from_groups`), with the rows DuckDB returns
  */
class EngineHavingTotalsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.HavingTotalsFromGroups))

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
    * The query matches DuckDB with the rewrite on and off; whether the rewrite computed the groups
    * at plan time
    */
  private def computesGroups(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    explain(disabled, query).linesIterator.exists(_.startsWith("Values")) shouldBe false
    explain(conformance.engine, query).linesIterator.exists(_.startsWith("Values"))

  private val germany =
    """from partsupp, supplier, nation
      |where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'"""
      .stripMargin

  test("total a decimal sum over the groups (TPC-H q11)") {
    val query =
      s"""${germany}
         |group by ps_partkey
         |where (ps_supplycost * ps_availqty).sum > {
         |  ${germany}
         |  select (ps_supplycost * ps_availqty).sum * 0.001
         |}
         |select ps_partkey, value = (ps_supplycost * ps_availqty).sum
         |order by value desc, ps_partkey
         |""".stripMargin
    computesGroups(query) shouldBe true
  }

  test("total min, max, and integer sums over the groups") {
    // One subquery per query: `prefetch_scalar_subqueries` evaluates several over one input together
    Seq(
      "s_acctbal.min < { from supplier select s_acctbal.min + 100 }",
      "s_acctbal.max > { from supplier select s_acctbal.max - 100 }",
      "s_suppkey.sum > { from supplier select s_suppkey.sum / 20 }",
      "s_acctbal.max - s_acctbal.min > { from supplier select (s_acctbal.max - s_acctbal.min) / 2 }"
    ).foreach { cond =>
      val query =
        s"""from supplier
           |group by s_nationkey
           |where ${cond}
           |select s_nationkey, s_acctbal.min as lo, s_acctbal.max as hi
           |order by s_nationkey
           |""".stripMargin
      computesGroups(query) shouldBe true
    }
  }

  test("total over an input without rows") {
    val query =
      """from supplier
        |where s_acctbal > 100000
        |group by s_nationkey
        |where s_acctbal.sum > { from supplier where s_acctbal > 100000 select s_acctbal.sum }
        |select s_nationkey, s_acctbal.sum as total
        |order by s_nationkey
        |""".stripMargin
    computesGroups(query) shouldBe true
  }

  test("evaluate subqueries whose values don't total over the groups on their own") {
    // avg and count don't total as themselves; a double sum would round differently
    Seq("s_acctbal.avg", "s_suppkey.count", "s_acctbal.to_double.sum / 25").foreach { value =>
      val query =
        s"""from supplier
           |group by s_nationkey
           |where s_acctbal.avg > { from supplier select ${value} }
           |select s_nationkey, s_acctbal.avg as a
           |order by s_nationkey
           |""".stripMargin
      computesGroups(query) shouldBe false
    }
  }

  test("evaluate a subquery over another input on its own") {
    val query =
      """from supplier
        |group by s_nationkey
        |where s_acctbal.sum > { from supplier where s_nationkey < 5 select s_acctbal.sum / 5 }
        |select s_nationkey, s_acctbal.sum as total
        |order by s_nationkey
        |""".stripMargin
    computesGroups(query) shouldBe false
  }

  test("evaluate subqueries on their own beside other subqueries or inside aggregate calls") {
    Seq(
      // An IN subquery, which binding the condition early would evaluate once more
      """s_acctbal.sum > { from supplier select s_acctbal.sum / 50 }
        |  and s_nationkey in (from nation where n_regionkey = 1 select n_nationkey)""".stripMargin,
      // A subquery inside an aggregate call, whose call changes once the value is known
      "(s_acctbal - { from supplier select s_acctbal.max }).sum < { from supplier select s_acctbal.max }"
    ).foreach { cond =>
      val query =
        s"""from supplier
           |group by s_nationkey
           |where ${cond}
           |select s_nationkey, s_acctbal.sum as total
           |order by s_nationkey
           |""".stripMargin
      computesGroups(query) shouldBe false
    }
  }

end EngineHavingTotalsTest
