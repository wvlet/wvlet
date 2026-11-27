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
  * Identical aggregating subqueries of a FROM list are computed once, as a shared CTE
  * (`share_duplicate_subqueries`), with the rows DuckDB returns
  */
class EngineDuplicateSubqueriesTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.ShareDuplicateSubqueries))

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

  private def readsCTE(plan: String): Boolean = plan.contains("CTE[__wv_subquery")

  /**
    * The query matches DuckDB with the rewrite on and off; whether the rewrite read the subqueries
    * as a shared CTE
    */
  private def sharesSubquery(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    readsCTE(explain(disabled, query)) shouldBe false
    readsCTE(explain(conformance.engine, query))

  private def query(second: String): String =
    s"""from {
       |  {
       |    from {
       |      {
       |        from lineitem
       |        where l_shipmode = 'AIR'
       |        group by l_suppkey, l_partkey
       |        select l_suppkey, l_partkey, sum(l_quantity) as qty
       |      }
       |    } as sa
       |    group by l_suppkey
       |    select l_suppkey, avg(qty) as ave
       |  }
       |} as sb, {
       |  {
       |    ${second}
       |  }
       |} as sc
       |where sb.l_suppkey = sc.l_suppkey and sc.qty > 2 * sb.ave
       |select sc.l_suppkey, sc.l_partkey, sc.qty
       |order by sc.l_suppkey, sc.l_partkey
       |""".stripMargin

  test("compute TPC-DS q65's twice-written aggregation once") {
    val q = query("""from lineitem
        |    where l_shipmode = 'AIR'
        |    group by l_suppkey, l_partkey
        |    select l_suppkey, l_partkey, sum(l_quantity) as qty""".stripMargin)
    sharesSubquery(q) shouldBe true
  }

  test("keep computing aggregations of different filters") {
    val q = query("""from lineitem
        |    where l_shipmode = 'RAIL'
        |    group by l_suppkey, l_partkey
        |    select l_suppkey, l_partkey, sum(l_quantity) as qty""".stripMargin)
    sharesSubquery(q) shouldBe false
  }

  test("keep computing aggregations of non-deterministic expressions") {
    // Each copy draws its own random() values, so the copies compute different rows
    val q = query("""from lineitem
        |    where l_shipmode = 'AIR'
        |    group by l_suppkey, l_partkey
        |    select l_suppkey, l_partkey, sum(l_quantity * random()) as qty""".stripMargin).replace(
      "sum(l_quantity)",
      "sum(l_quantity * random())"
    )
    readsCTE(explain(conformance.engine, q.replace("random()", "abs(0.5)"))) shouldBe true
    readsCTE(explain(conformance.engine, q)) shouldBe false
  }

  test("share copies that hold copies of another shared aggregation") {
    val a =
      """{
              |  from lineitem
              |  where l_shipmode = 'AIR'
              |  group by l_suppkey, l_partkey
              |  select l_suppkey, l_partkey, sum(l_quantity) as qty
              |}""".stripMargin
    val b =
      s"""{
               |  from ${a} as x
               |  group by l_suppkey
               |  select l_suppkey, avg(qty) as ave
               |}""".stripMargin
    val q =
      s"""from ${a} as a0, ${b} as b1, ${b} as b2
               |where a0.l_suppkey = b1.l_suppkey and b1.l_suppkey = b2.l_suppkey
               |  and a0.qty > 2 * b2.ave
               |select a0.l_suppkey, a0.l_partkey, a0.qty, b1.ave
               |order by a0.l_suppkey, a0.l_partkey
               |""".stripMargin
    sharesSubquery(q) shouldBe true
    // b1 and b2 are one CTE; a0 is the only other copy of `a`, which isn't shared on its own
    val plan = explain(conformance.engine, q)
    plan.contains("CTE[__wv_subquery2") shouldBe false
  }

end EngineDuplicateSubqueriesTest
