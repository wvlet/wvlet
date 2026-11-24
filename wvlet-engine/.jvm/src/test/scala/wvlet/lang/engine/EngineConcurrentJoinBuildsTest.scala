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
import wvlet.lang.engine.exec.MorselOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** The hash join builds of one pipeline run at the same time (`concurrent_join_builds`) */
class EngineConcurrentJoinBuildsTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(
    options.withDisabled(Optimization.ConcurrentJoinBuilds)
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

  /** The most prepare steps of a morsel pipeline in the plan */
  private def maxPrepareSteps(op: Operator): Int =
    val own =
      op match
        case m: MorselOp =>
          m.prepareSteps
        case _ =>
          0
    (op.children ++ op.sharedInputs).map(maxPrepareSteps).foldLeft(own)(math.max)

  // A probe pipeline that waits for several builds (orders, customer, nation)
  private val query =
    """select n_name, count(*) as n, sum(l_extendedprice) as revenue
      |from lineitem
      |  join orders on l_orderkey = o_orderkey
      |  join customer on o_custkey = c_custkey
      |  join nation on c_nationkey = n_nationkey
      |where l_returnflag = 'R'
      |group by n_name
      |order by revenue desc, n_name""".stripMargin

  test("should return the same rows as building one join after another") {
    val r = rows(engine, query)
    r.nonEmpty shouldBe true
    r shouldBe rows(disabled, query)
  }

  test("should run the builds of a pipeline as one step") {
    if TaskRunner.default.parallelism <= 1 then
      ignore("Plans are not parallelized on one processor")
    withQuery(query) { q =>
      val on  = engine.physicalPlan(q)
      val off = disabled.physicalPlan(q)
      try
        (maxPrepareSteps(off) >= 2) shouldBe true
        maxPrepareSteps(on) shouldBe 1
      finally
        on.close()
        off.close()
    }
  }

end EngineConcurrentJoinBuildsTest
