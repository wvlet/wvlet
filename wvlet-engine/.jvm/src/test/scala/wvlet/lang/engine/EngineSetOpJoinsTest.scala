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
  * INTERSECT / EXCEPT (without ALL) run as semi / anti hash joins with NULLs equal
  * (`set_op_joins`), with the rows DuckDB returns
  */
class EngineSetOpJoinsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.SetOpJoins))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /**
    * The query matches DuckDB with the rewrite on and off, and returns the plan with it on and the
    * engine options, and the plan with `off` disabled
    */
  private def checkPlans(query: String, off: Optimization): (String, String) =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val rel       = GenSQL.expand(q, summon[Context])
    conformance.compare(query, rel, target = disabled).matched shouldBe true
    val plan = conformance.engine.explain(rel)
    debug(plan)
    (plan, conformance.engine.withOptimizer(options.withDisabled(off)).explain(rel))

  /** The query matches DuckDB with the rewrite on and off, and returns the plan with it on */
  private def check(query: String): String = checkPlans(query, Optimization.SetOpJoins)._1

  // Nation keys with NULLs (keys 0-4) and duplicates (key % 7)
  private val nullableKeys =
    """select if n_nationkey < 5 then null else n_nationkey % 7 as k, n_regionkey as r"""

  test("run EXCEPT as an anti join with NULLs equal") {
    val plan = check(s"""from nation
         |${nullableKeys}
         |except {
         |  from nation
         |  where n_nationkey < 3 or n_nationkey > 20
         |  ${nullableKeys}
         |}
         |""".stripMargin)
    plan shouldContain "HashJoin[Anti]"
    plan shouldContain "nulls equal"
    plan shouldNotContain "Except"
  }

  test("give EXCEPT's anti join the planner's miss filter, and drop it with the toggle off") {
    val query =
      s"""from nation
         |${nullableKeys}
         |except {
         |  from nation
         |  where n_nationkey < 3
         |  ${nullableKeys}
         |}
         |""".stripMargin
    val (on, off) = checkPlans(query, Optimization.ProbeMissFilter)
    on shouldContain "HashJoin[Anti](#0,#1 = #0,#1, nulls equal, miss filter)"
    off shouldContain "HashJoin[Anti]"
    off shouldNotContain "miss filter"
  }

  test("run INTERSECT as a semi join with NULLs equal") {
    val plan = check(s"""from nation
         |${nullableKeys}
         |intersect {
         |  from nation
         |  where n_nationkey < 3 or n_nationkey > 20
         |  ${nullableKeys}
         |}
         |""".stripMargin)
    plan shouldContain "HashJoin[Semi]"
    plan shouldNotContain "Intersect"
  }

  test("chain EXCEPTs over large inputs, as TPC-DS q87") {
    val plan = check("""from {
        |  from lineitem
        |  select l_suppkey, l_returnflag
        |  except {
        |    from partsupp
        |    where ps_availqty < 3000
        |    select ps_suppkey, 'N'
        |  }
        |  except {
        |    from supplier
        |    where s_acctbal < 0
        |    select s_suppkey, 'R'
        |  }
        |}
        |select count(*) as n
        |""".stripMargin)
    plan.linesIterator.count(_.contains("HashJoin[Anti]")) shouldBe 2
  }

  test("compare columns of different types in the set operation's types") {
    check("""from orders
        |select o_custkey
        |intersect {
        |  from customer
        |  where c_acctbal > 5000
        |  select c_nationkey::bigint * 10 as o_custkey
        |}
        |""".stripMargin)
  }

  test("keep EXCEPT ALL and INTERSECT ALL as set operations") {
    val plan = check(s"""from nation
         |${nullableKeys}
         |except all {
         |  from nation
         |  where n_nationkey > 20
         |  ${nullableKeys}
         |}
         |""".stripMargin)
    plan shouldContain "Except"
  }

end EngineSetOpJoinsTest
