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
  * A top-N reads a scan's pass-through strings for its N rows only, from a second scan joined on
  * the scan's declared key (`late_columns`), with the rows DuckDB returns
  */
class EngineLateColumnsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options, declaredKeys = true)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.LateColumns))

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

  /** The query matches DuckDB with and without the rewrite, and returns both plans */
  private def check(query: String): (String, String) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val sql       = GenSQL.expand(q, summon[Context])
    val on        = conformance.compare(query, sql, target = conformance.engine)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val off = conformance.compare(query, sql, target = disabled)
    off.matched shouldBe true
    (explain(conformance.engine, query), explain(disabled, query))

  private def customerScans(plan: String): Int = plan
    .linesIterator
    .count(_.contains("Scan[customer]"))

  test("read the customer strings of the top orders only") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |select o_orderkey, c_custkey, c_name, c_address, c_comment, o_totalprice
        |order by o_totalprice desc
        |limit 3
        |""".stripMargin)
    customerScans(on) shouldBe 2
    customerScans(off) shouldBe 1
    // The first customer scan no longer reads the strings
    on.linesIterator.exists(l => l.contains("Scan[customer]") && !l.contains("c_name")) shouldBe
      true
  }

  test("keep the strings a sort key reads") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |select o_orderkey, c_name, c_comment, o_totalprice
        |order by c_name, o_totalprice desc
        |limit 3
        |""".stripMargin)
    on shouldBe off
  }

  test("keep the strings when the key doesn't reach the sort") {
    val (on, off) = check("""from orders, customer
        |where o_custkey = c_custkey
        |select o_orderkey, c_name, o_totalprice
        |order by o_totalprice desc
        |limit 3
        |""".stripMargin)
    on shouldBe off
  }

  test("keep the strings when no join reads the key, which may then be NULL") {
    val (on, off) = check("""from customer
        |select c_custkey, c_name, c_comment, c_acctbal
        |order by c_acctbal desc
        |limit 2
        |""".stripMargin)
    on shouldBe off
  }

  test("read late strings of a lookup under an aggregation's sort (TPC-H q10)") {
    val (on, off) = check("""from customer, orders, lineitem, nation
        |where c_custkey = o_custkey and l_orderkey = o_orderkey
        |  and l_returnflag = 'R' and c_nationkey = n_nationkey
        |group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        |select c_custkey, c_name, revenue = (l_extendedprice * (1 - l_discount)).sum,
        |  c_acctbal, n_name, c_address, c_phone, c_comment
        |order by revenue desc
        |limit 2
        |""".stripMargin)
    customerScans(on) shouldBe 2
    customerScans(off) shouldBe 1
  }

end EngineLateColumnsTest
