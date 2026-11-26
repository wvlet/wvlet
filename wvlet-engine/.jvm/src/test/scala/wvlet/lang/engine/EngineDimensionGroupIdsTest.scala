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
  * Aggregations grouped by a dimension's strings group by a representative key of them instead
  * (`dimension_group_ids`), with the rows DuckDB returns
  */
class EngineDimensionGroupIdsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options, declaredKeys = true)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.DimensionGroupIds))

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

  // Without eager aggregation, which would sum the order lines per part before the part join
  private val noEager = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.EagerAggregation))

  private val noEagerOff = conformance
    .engine
    .withOptimizer(
      options.withDisabled(Optimization.EagerAggregation, Optimization.DimensionGroupIds)
    )

  /** The query matches DuckDB on `onEngine` and `offEngine`, and returns their plans */
  private def check(
      query: String,
      onEngine: WvletEngine = conformance.engine,
      offEngine: WvletEngine = disabled
  ): (String, String) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val sql       = GenSQL.expand(q, summon[Context])
    val on        = conformance.compare(query, sql, target = onEngine)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val off = conformance.compare(query, sql, target = offEngine)
    off.matched shouldBe true
    (explain(onEngine, query), explain(offEngine, query))

  private def reps(plan: String): Int = plan.linesIterator.count(_.contains("win:min("))

  private val byBrand =
    """from lineitem, part
      |where l_partkey = p_partkey
      |group by p_mfgr, p_brand, l_returnflag
      |select p_mfgr, p_brand, l_returnflag, sum(l_quantity) as q, count(*) as n
      |order by p_mfgr, p_brand, l_returnflag
      |""".stripMargin

  test("group order lines by a part rep instead of the part's manufacturer and brand") {
    val (on, off) = check(byBrand, noEager, noEagerOff)
    // One window for the lookup; the strings come back from an aggregation of part
    reps(on) shouldBe 1
    reps(off) shouldBe 0
  }

  test("group by the reps of two dimensions") {
    val (on, _) = check("""from lineitem, part, supplier
        |where l_partkey = p_partkey and l_suppkey = s_suppkey
        |group by p_mfgr, s_name
        |select p_mfgr, s_name, sum(l_extendedprice) as p
        |order by p_mfgr, s_name
        |""".stripMargin)
    reps(on) shouldBe 2
  }

  test("keep the strings that a call reads") {
    val (on, off) = check("""from lineitem, part
        |where l_partkey = p_partkey
        |group by p_brand
        |select p_brand, max(p_brand) as m, sum(l_quantity) as q
        |order by p_brand
        |""".stripMargin)
    on shouldBe off
  }

  test("keep the strings that a computed key reads") {
    val noDependent = conformance
      .engine
      .withOptimizer(options.withDisabled(Optimization.DependentGroupKeys))
    val noDependentOff = conformance
      .engine
      .withOptimizer(
        options.withDisabled(Optimization.DependentGroupKeys, Optimization.DimensionGroupIds)
      )
    val (on, off) = check(
      """from lineitem, part
        |where l_partkey = p_partkey
        |group by p_mfgr, lower(p_mfgr)
        |select p_mfgr, lower(p_mfgr) as m, sum(l_quantity) as q
        |order by p_mfgr
        |""".stripMargin,
      noDependent,
      noDependentOff
    )
    reps(on) shouldBe 0
  }

end EngineDimensionGroupIdsTest
