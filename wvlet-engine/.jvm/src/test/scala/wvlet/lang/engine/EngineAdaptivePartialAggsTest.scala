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
import wvlet.lang.engine.exec.HashAggOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * `adaptive_partial_aggs` reaches the partial aggregates of two-phase aggregations; their results
  * are checked in PartialAggSkipTest
  */
class EngineAdaptivePartialAggsTest extends UniTest:
  test("give the partial aggregates of a two-phase aggregation a skip decision") {
    val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    def skips(o: OptimizerOptions): Seq[Boolean] =
      val unit = CompilationUnit.fromWvletString("""from 'spec/tpch/data/lineitem.parquet'
          |group by l_orderkey
          |agg sum(l_quantity) as q""".stripMargin)
      val r         = compiler.compileSingleUnit(unit)
      given Context = r.context
      val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
      def collect(op: Operator): Seq[Boolean] =
        (
          op match
            case a: HashAggOp =>
              Seq(a.skip != null)
            case _ =>
              Nil
        ) ++ op.children.flatMap(collect)
      collect(WvletEngine(optimizer = o).physicalPlan(GenSQL.expand(query, summon[Context])))
    val options = OptimizerOptions()
    skips(options) shouldContain true
    skips(options.withDisabled(Optimization.AdaptivePartialAggs)).contains(true) shouldBe false
  }

end EngineAdaptivePartialAggsTest
