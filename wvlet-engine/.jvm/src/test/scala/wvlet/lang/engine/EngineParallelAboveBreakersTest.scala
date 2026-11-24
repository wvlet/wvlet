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
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * The operators above grouped aggregations run as morsels over the aggregation's output batches
  * (`parallel_above_breakers`)
  */
class EngineParallelAboveBreakersTest extends UniTest:
  private val options  = OptimizerOptions().withValidatePlans(true)
  private val engine   = WvletEngine(optimizer = options)
  private val disabled = engine.withOptimizer(
    options.withDisabled(Optimization.ParallelAboveBreakers)
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

  /** Partition grouped aggregations at any size, so that their output has a batch per partition */
  private def partitioned[A](body: => A): A =
    val key  = "wvlet.engine.partitionedAggMinRows"
    val prev = sys.props.get(key)
    sys.props(key) = "1"
    try body
    finally
      prev match
        case Some(v) =>
          sys.props(key) = v
        case None =>
          sys.props -= key

  private def rows(e: WvletEngine, query: String): Seq[String] = partitioned {
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
    }
  }

  private def explain(e: WvletEngine, query: String): String = partitioned {
    withQuery(query)(q => e.explain(q))
  }

  // A join probe, a filter, and a projection over an aggregation, with no ORDER BY: the rows must
  // come out in the same order as on the calling thread
  private val query =
    """select o.o_custkey, a.l_orderkey, a.q * 2 as q2
      |from (select l_orderkey, sum(l_quantity) as q from lineitem group by l_orderkey) a
      |join orders o on a.l_orderkey = o.o_orderkey
      |where a.q > 100""".stripMargin

  // An aggregation over an aggregation, and a left join with unmatched rows from both readers
  private val nested =
    """select n, count(*) as c, sum(q) as s from (
      |  select l_orderkey, count(*) as n, sum(l_quantity) as q from lineitem group by l_orderkey
      |) t left join orders o on t.l_orderkey = o.o_orderkey and o.o_orderstatus = 'F'
      |group by n""".stripMargin

  test("should return the same rows in the same order as running the operators on one thread") {
    for q <- Seq(query, nested) do
      val r = rows(engine, q)
      r.nonEmpty shouldBe true
      r shouldBe rows(disabled, q)
  }

  test("should run the probe above an aggregation as morsels over its output") {
    if TaskRunner.default.parallelism <= 1 then
      ignore("Plans are not parallelized on one processor")
    val plan = explain(engine, query)
    debug(plan)
    plan shouldContain "Scan[Aggregate]"
    explain(disabled, query) shouldNotContain "Scan[Aggregate]"
  }

  test("should estimate the rows above an aggregation as without the morsels") {
    // Subquery plans read these estimates to choose join build sides
    def estimate(e: WvletEngine): Double = partitioned {
      withQuery(query) { q =>
        val op = e.physicalPlan(q)
        try CardinalityEstimator.rows(op)
        finally op.close()
      }
    }
    estimate(engine) shouldBe estimate(disabled)
  }

end EngineParallelAboveBreakersTest
