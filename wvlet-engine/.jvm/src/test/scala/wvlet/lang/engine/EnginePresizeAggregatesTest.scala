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
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/** Hash aggregations pre-size their group tables from the planner's group estimates */
class EnginePresizeAggregatesTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  private val off     = engine.withOptimizer(options.withDisabled(Optimization.PresizeAggregates))
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The expected group counts of the aggregations with keys, and the query's rows */
  private def run(e: WvletEngine, sql: String): (Seq[Int], Seq[IndexedSeq[Any]]) =
    val unit      = CompilationUnit.fromSqlString(sql)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    val op        = e.physicalPlan(GenSQL.expand(q, summon[Context]))
    def hints(o: Operator): Seq[Int] =
      val own =
        o match
          case a: HashAggOp if a.keys.nonEmpty =>
            Seq(a.expectedGroups)
          case a: PartitionedAggOp =>
            Seq(a.expectedGroups)
          case _ =>
            Nil
      // An aggregation read as morsels is a shared input of its scan
      own ++ (o.children ++ o.sharedInputs).flatMap(hints)
    val rows = op
      .drain()
      .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toIndexedSeq))
    op.close()
    (hints(op), rows)

  private val query = "select l_orderkey, count(*) from lineitem group by l_orderkey order by 1"

  test("pre-size an aggregation by keys with source statistics") {
    val (on, rows) = run(engine, query)
    debug(on)
    on.exists(_ > 0) shouldBe true
    on.forall(_ <= GroupTable.MaxPresizeGroups) shouldBe true
    val (disabled, offRows) = run(off, query)
    disabled.forall(_ == 0) shouldBe true
    rows shouldBe offRows
  }

end EnginePresizeAggregatesTest
