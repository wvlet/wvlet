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
  * An uncorrelated IN over an inner join tree whose subquery is estimated larger than the input it
  * reads is computed at plan time and placed on that input by its exact row count
  * (`count_in_subqueries`), with the rows DuckDB returns
  */
class EngineCountInSubqueriesTest extends UniTest:
  // reuse_in_subquery_aggregates would drop the q18-shaped IN these tests place
  private val options = OptimizerOptions()
    .withValidatePlans(true)
    .withDisabled(Optimization.ReuseInSubqueryAggregates)

  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.CountInSubqueries))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The plan with the toggle on and off, after checking both return DuckDB's rows */
  private def plans(query: String): (String, String) =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val sql       = GenSQL.expand(q, summon[Context])
    conformance.compare(query, sql, target = disabled).matched shouldBe true
    val plan = (conformance.engine.explain(sql), disabled.explain(sql))
    debug(plan)
    plan

  /** Whether the plan's semi join runs below (before) its first inner join */
  private def semiBelowInner(plan: String): Boolean =
    val lines = plan.linesIterator.toIndexedSeq
    val semi  = lines.indexWhere(_.contains("HashJoin[Semi]"))
    val inner = lines.indexWhere(_.contains("HashJoin[Inner]"))
    semi >= 0 && inner >= 0 && semi < inner

  test("place an IN over a group-by having on its input by the exact row count") {
    val (on, off) = plans("""from customer, orders, lineitem
        |where o_orderkey in (
        |    from lineitem
        |    group by l_orderkey
        |    where l_quantity.sum > 250
        |    select l_orderkey
        |  )
        |  and c_custkey = o_custkey
        |  and o_orderkey = l_orderkey
        |group by o_orderkey
        |select o_orderkey, l_quantity.sum as q
        |""".stripMargin)
    semiBelowInner(on) shouldBe true
    on shouldContain "Values"
    semiBelowInner(off) shouldBe false
  }

  test("keep an IN whose subquery returns more rows than its input over the join tree") {
    val (on, _) = plans("""from customer, orders
        |where o_orderkey in (from lineitem select l_orderkey)
        |  and c_custkey = o_custkey
        |select count(*) as n
        |""".stripMargin)
    // Computed, but larger than orders: joined over the tree with the computed rows
    on shouldContain "Values"
    semiBelowInner(on) shouldBe false
  }

end EngineCountInSubqueriesTest
