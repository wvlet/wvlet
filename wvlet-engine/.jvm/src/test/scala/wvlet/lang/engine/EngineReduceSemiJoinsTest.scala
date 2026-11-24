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
  * Inner joins whose rows only decide membership become semi joins (`reduce_semi_joins`), with the
  * rows DuckDB returns
  */
class EngineReduceSemiJoinsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.ReduceSemiJoins))

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

  /** The query matches DuckDB with the rewrite on and off, and returns the plan with it on */
  private def check(query: String): String =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    explain(conformance.engine, query)

  private def semiJoins(plan: String): Int = plan
    .linesIterator
    .count(l => l.contains("HashJoin[Semi]") || l.contains("HashJoin[RightSemi]"))

  test("turn an inner join under IN into a semi join when the subquery reads one side") {
    val query =
      """from orders
        |where o_orderkey in ({
        |  from lineitem, part
        |  where l_partkey = p_partkey and p_size < 20
        |  select l_orderkey
        |})
        |group by o_orderpriority
        |select o_orderpriority, count(*) as n
        |""".stripMargin
    val plan = check(query)
    semiJoins(plan) shouldBe semiJoins(explain(disabled, query)) + 1
  }

  test("keep a cross-side non-equality as the residual of the semi join") {
    val query =
      """from orders
        |where o_orderkey in ({
        |  from lineitem as l1, lineitem as l2
        |  where l1.l_orderkey = l2.l_orderkey and l1.l_suppkey != l2.l_suppkey
        |  select l1.l_orderkey
        |})
        |select count(*) as n
        |""".stripMargin
    check(query) shouldContain "residual="
  }

  test("reduce a shared CTE whose readers all test membership") {
    val query =
      """with t as {
        |  from lineitem as l1, lineitem as l2
        |  where l1.l_orderkey = l2.l_orderkey and l1.l_suppkey != l2.l_suppkey
        |  select l1.l_orderkey, l1.l_suppkey as s1, l2.l_suppkey as s2
        |}
        |from orders
        |where o_orderkey in ({ from t select l_orderkey })
        |  and o_orderkey in ({
        |    from part, t, lineitem
        |    where p_partkey = l_partkey and lineitem.l_orderkey = t.l_orderkey and p_size < 30
        |    select t.l_orderkey
        |  })
        |select count(*) as n, sum(o_totalprice) as total
        |""".stripMargin
    val plan = check(query)
    plan shouldContain "residual="
  }

  test("keep a shared CTE's duplicates when one reader counts them") {
    val query =
      """with t as {
        |  from lineitem as l1, lineitem as l2
        |  where l1.l_orderkey = l2.l_orderkey and l1.l_suppkey != l2.l_suppkey
        |  select l1.l_orderkey
        |}
        |from orders
        |where o_orderkey in ({ from t select l_orderkey })
        |  and o_totalprice > ({ from t select count(*) as c })
        |select count(*) as n
        |""".stripMargin
    check(query) shouldNotContain "residual="
  }

  test("keep a shared CTE's duplicates when one reader joins it") {
    val query =
      """with t as {
        |  from lineitem as l1, lineitem as l2
        |  where l1.l_orderkey = l2.l_orderkey and l1.l_suppkey != l2.l_suppkey
        |  select l1.l_orderkey
        |}
        |from orders, t
        |where o_orderkey = t.l_orderkey
        |  and o_orderkey in ({ from t select l_orderkey })
        |select count(*) as n
        |""".stripMargin
    check(query) shouldNotContain "residual="
  }

  test("reduce the joins under DISTINCT") {
    val plan = check("""from lineitem, orders
        |where l_orderkey = o_orderkey and o_orderpriority = '1-URGENT'
        |select distinct l_suppkey
        |""".stripMargin)
    semiJoins(plan) shouldBe 1
  }

  test("not reduce the joins below an aggregation that counts rows") {
    val query =
      """from orders
        |where o_orderkey in ({
        |  from lineitem, partsupp
        |  where l_partkey = ps_partkey
        |  group by l_orderkey
        |  where l_quantity.sum > 1000
        |  select l_orderkey
        |})
        |select count(*) as n
        |""".stripMargin
    val plan = check(query)
    semiJoins(plan) shouldBe semiJoins(explain(disabled, query))
  }

  test("not reduce the joins below a window function") {
    val query =
      """from orders
        |where o_orderkey in ({
        |  from lineitem, part
        |  where l_partkey = p_partkey
        |  select l_orderkey, row_number() over (partition by l_orderkey order by p_size) as rn
        |  where rn = 3
        |  select l_orderkey
        |})
        |select count(*) as n
        |""".stripMargin
    val plan = check(query)
    semiJoins(plan) shouldBe semiJoins(explain(disabled, query))
  }

  test("not reduce the joins below a top-N, which keeps duplicates") {
    val query =
      """from orders
        |where o_orderkey in ({
        |  from lineitem, part
        |  where l_partkey = p_partkey
        |  order by l_orderkey
        |  limit 5
        |  select l_orderkey
        |})
        |select count(*) as n
        |""".stripMargin
    val plan = check(query)
    semiJoins(plan) shouldBe semiJoins(explain(disabled, query))
  }

  test("match DuckDB for NOT IN over a join") {
    check("""from orders
        |where o_custkey not in ({
        |  from customer, nation
        |  where c_nationkey = n_nationkey and n_regionkey = 1
        |  select c_custkey
        |})
        |select count(*) as n
        |""".stripMargin)
  }

  test("match DuckDB for NOT EXISTS over a join") {
    check("""from orders
        |where not exists {
        |  from lineitem, part
        |  where l_partkey = p_partkey and l_orderkey = o_orderkey and p_size > 40
        |}
        |select count(*) as n
        |""".stripMargin)
  }

end EngineReduceSemiJoinsTest
