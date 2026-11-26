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
  * A min, max, or avg per key over the same rows that a join reads it back into becomes a window
  * over the join's rows (`aggregate_windows`), with the rows DuckDB returns
  */
class EngineAggregateWindowsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.AggregateWindows))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def explain(e: WvletEngine, query: String): String =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    e.explain(GenSQL.expand(q, summon[Context]))

  /** Whether the rule rewrites the query, which matches DuckDB with the rule on and off */
  private def windowed(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    explain(disabled, query) shouldNotContain "Window["
    explain(conformance.engine, query).contains("Window[")

  private def tpch(name: String): Unit =
    val unit = conformance.units.find(_.sourceFile.fileName == name).get
    conformance.queries(unit) { (q, rel) =>
      conformance.assertMatches(q, rel)
      conformance.engine.explain(rel) shouldContain "Window["
      disabled.explain(rel) shouldNotContain "Window["
    }

  test("compute TPC-H q17's avg per part over the lineitem rows of the outer join") {
    tpch("q17.wv")
  }

  test("compute TPC-H q2's min supply cost per part over the outer join's rows") {
    tpch("q2.wv")
  }

  test("compute a max in the select list, decorrelated as a left join") {
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23'
        |select l_orderkey, l_linenumber, {
        |  from lineitem as l2
        |  where l2.l_partkey = p_partkey
        |  select l2.l_quantity.max
        |} as mx
        |""".stripMargin
    windowed(query) shouldBe true
  }

  test("leave a sum, which counts each row once per matching outer row") {
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23'
        |  and l_quantity * 10 < {
        |    from lineitem as l2
        |    where l2.l_partkey = p_partkey
        |    select l2.l_quantity.sum
        |  }
        |select l_extendedprice.sum as total
        |""".stripMargin
    windowed(query) shouldBe false
  }

  test("leave an outer query that keeps fewer rows of the table than the subquery") {
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23' and l_shipmode = 'AIR'
        |  and l_quantity < {
        |    from lineitem as l2
        |    where l2.l_partkey = p_partkey
        |    select 0.5 * l2.l_quantity.avg
        |  }
        |select l_extendedprice.sum as total
        |""".stripMargin
    windowed(query) shouldBe false
  }

  test("leave a subquery that keeps fewer rows of the table than the outer query") {
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23'
        |  and l_quantity < {
        |    from lineitem as l2
        |    where l2.l_partkey = p_partkey and l2.l_shipmode = 'AIR'
        |    select l2.l_quantity.avg
        |  }
        |select l_extendedprice.sum as total
        |""".stripMargin
    windowed(query) shouldBe false
  }

  test("leave an outer join on another column than the subquery's key") {
    // The parts of a brand keep the lineitem rows of some parts only, so the rows of a supplier in
    // the join are not all of its rows
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey and p_brand = 'Brand#23'
        |  and l_quantity < {
        |    from lineitem as l2
        |    where l2.l_suppkey = l_suppkey
        |    select l2.l_quantity.avg
        |  }
        |select l_extendedprice.sum as total
        |""".stripMargin
    windowed(query) shouldBe false
  }

end EngineAggregateWindowsTest
