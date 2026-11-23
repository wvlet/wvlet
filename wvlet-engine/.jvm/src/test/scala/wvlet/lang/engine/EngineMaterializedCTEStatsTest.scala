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

/** Joins over shared CTEs ordered again with the CTEs' exact row counts */
class EngineMaterializedCTEStatsTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(
    options.withDisabled(Optimization.ReplanMaterializedCTEs)
  )

  private val rootOnly = engine.withOptimizer(
    options.withDisabled(Optimization.ReplanBoundCTEJoins)
  )

  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def explain(e: WvletEngine, query: String): String =
    val plan = withQuery(query)(q => e.explain(q))
    debug(plan)
    plan

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  // Two rows (one per l_linestatus), which the planner estimated as 1e6 before it was computed
  private val cte =
    """with t as (
      |  select l_linestatus as s, count(*) as c from lineitem group by l_linestatus
      |)
      |""".stripMargin

  /**
    * The first source of a flow-style plan at the top level: the probe side of its first join (a
    * shared body printed before it is indented)
    */
  private def firstScan(plan: String): String = plan
    .linesIterator
    .find(!_.startsWith(" "))
    .getOrElse("")
    .stripPrefix("↙ ")
    .takeWhile(_ != '(')

  test("build hash joins on a small shared CTE once its rows are known") {
    val query =
      s"""${cte}select o.o_orderkey, a.c, b.c
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s""".stripMargin
    // With the 1e6 estimate, the CTE reads are the probe side; with two rows, orders is
    firstScan(explain(disabled, query)) shouldBe "Scan[CTE[t]]"
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("re-plan joins whose CTE readers' filters were pushed into the body") {
    val query =
      s"""${cte}select o.o_orderkey, a.c, b.c
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |where a.s = 'F' and b.s in ('F', 'O')""".stripMargin
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("keep the plan when the exact rows don't change the join order") {
    val query = s"""${cte}select a.s, a.c, b.c from t a join t b on a.s = b.s""".stripMargin
    explain(engine, query) shouldBe explain(disabled, query)
    rows(engine, query) shouldBe rows(disabled, query)
  }

  test("re-plan joins of an aggregation input with the CTE's exact rows") {
    // The binder orders the aggregation input's joins before the CTE is computed
    val query =
      s"""${cte}select a.s, count(*) as n, sum(b.c) as m
         |from orders o join t a on o.o_orderstatus = a.s join t b on a.s = b.s
         |group by a.s""".stripMargin
    firstScan(explain(rootOnly, query)) shouldBe "Scan[CTE[t]]"
    firstScan(explain(engine, query)) shouldBe "Scan[orders]"
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("keep the column order of a re-planned input read by position") {
    val query =
      s"""${cte}select * from (
         |  select o.o_orderkey, a.c from orders o join t a on o.o_orderstatus = a.s
         |  join t b on a.s = b.s group by o.o_orderkey, a.c
         |  union all
         |  select 0, c from t
         |)""".stripMargin
    explain(engine, query) shouldNotBe explain(rootOnly, query)
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

end EngineMaterializedCTEStatsTest
