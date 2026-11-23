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
import wvlet.lang.engine.exec.StartSharedOp
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * Shared CTE results computed at the same time on coordinator threads (`concurrent_shared_ctes`)
  */
class EngineConcurrentSharedCTEsTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(
    options.withDisabled(Optimization.ConcurrentSharedCTEs)
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

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
    }

  // Two independent CTEs, and a third that joins them; each is read twice
  private val query =
    """with a as (
      |  select l_orderkey, sum(l_quantity) as q from lineitem group by l_orderkey
      |),
      |b as (
      |  select o_orderkey, o_custkey from orders where o_orderdate < date '1995-01-01'
      |),
      |c as (
      |  select a.l_orderkey, a.q, b.o_custkey from a join b on a.l_orderkey = b.o_orderkey
      |)
      |select x.o_custkey, count(*) as n, sum(x.q) as q from c x join c y on x.o_custkey = y.o_custkey
      |  where x.q > (select avg(q) from a) and y.l_orderkey in (select o_orderkey from b)
      |    and y.l_orderkey in (select l_orderkey from a)
      |  group by x.o_custkey
      |  order by n desc, x.o_custkey
      |  limit 20""".stripMargin

  // Shared CTEs that no join reads, so they are computed when the plan runs
  private val unionQuery =
    """with a as (
      |  select l_orderkey, sum(l_quantity) as q from lineitem group by l_orderkey
      |),
      |b as (
      |  select o_custkey, count(*) as n from orders group by o_custkey
      |)
      |select 'a' as k, count(*) as n, sum(q) as s from a
      |union all select 'a_max', count(*), max(q) from a where q > 100
      |union all select 'b', count(*), sum(n) from b
      |union all select 'b_max', count(*), max(n) from b where n > 10""".stripMargin

  test("should return the same rows in the same order as computing shared CTEs one by one") {
    for q <- Seq(query, unionQuery) do
      val r = rows(engine, q)
      r.nonEmpty shouldBe true
      r shouldBe rows(disabled, q)
  }

  test("should start the shared CTE results when the plan starts running") {
    if TaskRunner.default.parallelism <= 1 then
      ignore("Shared results are computed one by one on one processor")
    withQuery(unionQuery) { q =>
      val op = engine.physicalPlan(q)
      try
        op shouldMatch { case _: StartSharedOp =>
        }
        // EXPLAIN shows the plan without the starter
        op.explain() shouldBe disabled.explain(q)
      finally
        op.close()
      val off = disabled.physicalPlan(q)
      try
        off shouldMatch {
          case o if !o.isInstanceOf[StartSharedOp] =>
        }
      finally off.close()
    }
  }

end EngineConcurrentSharedCTEsTest
