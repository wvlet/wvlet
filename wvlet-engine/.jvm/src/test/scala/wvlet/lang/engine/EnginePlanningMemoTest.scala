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
import wvlet.lang.engine.plan.ColumnNotFound
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * The planner tells whether a subquery is correlated once per subquery, and the binder's
  * missing-column signal carries no stack trace (`planning_memo`), with the same plans and the rows
  * DuckDB returns
  */
class EnginePlanningMemoTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.PlanningMemo))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The correlation checks of planning the query, its plans, and whether its rows match DuckDB */
  private def check(query: String): (Int, Int) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val sql       = GenSQL.expand(q, summon[Context])
    conformance.compare(query, sql, target = conformance.engine).matched shouldBe true
    conformance.compare(query, sql, target = disabled).matched shouldBe true
    conformance.engine.explain(sql) shouldBe disabled.explain(sql)
    def checks(e: WvletEngine): Int =
      val planner = e.newPlanner
      planner.plan(sql).close()
      planner.correlationChecks
    (checks(conformance.engine), checks(disabled))

  test("check a correlated scalar subquery once (TPC-H q17)") {
    val (on, off) = check("""from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
        |  and l_quantity < (
        |    from lineitem
        |    where l_partkey = p_partkey
        |    select 0.2 * l_quantity.avg
        |  )
        |select avg_yearly = l_extendedprice.sum / 7.0
        |""".stripMargin)
    on shouldBe 1
    (off > on) shouldBe true
  }

  test("check an uncorrelated scalar subquery once, as before") {
    val (on, off) = check("""from orders
        |where o_totalprice > (from orders select o_totalprice.avg)
        |select cnt = o_orderkey.count
        |""".stripMargin)
    // Asked only once while planning, with or without the memo
    on shouldBe 1
    off shouldBe 1
  }

  test("throw a missing column without a stack trace when enabled") {
    ColumnNotFound("Column not found: x", stackTrace = false).getStackTrace.isEmpty shouldBe true
    ColumnNotFound("Column not found: x").getStackTrace.nonEmpty shouldBe true
  }

end EnginePlanningMemoTest
