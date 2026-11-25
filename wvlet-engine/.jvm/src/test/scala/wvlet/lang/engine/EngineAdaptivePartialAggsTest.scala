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
import wvlet.lang.engine.exec.PartitionedAggOp
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * `adaptive_partial_aggs` reaches the partial aggregates of two-phase aggregations, and
  * `pack_agg_keys` the partitioned aggregations; their results are checked in PartialAggSkipTest
  * and AggKeyPackingTest
  */
class EngineAdaptivePartialAggsTest extends UniTest:
  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
  private val options  = OptimizerOptions()

  /** `flag` of each operator it applies to in the physical plan of a grouped lineitem query */
  private def flags(o: OptimizerOptions, flag: PartialFunction[Operator, Boolean]): Seq[Boolean] =
    val unit = CompilationUnit.fromWvletString("""from 'spec/tpch/data/lineitem.parquet'
        |group by l_orderkey, l_linenumber
        |agg sum(l_quantity) as q""".stripMargin)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val query     = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
    def collect(op: Operator): Seq[Boolean] = flag.lift(op).toSeq ++ op.children.flatMap(collect)
    collect(WvletEngine(optimizer = o).physicalPlan(GenSQL.expand(query, summon[Context])))

  test("give the partial aggregates of a two-phase aggregation a skip decision") {
    val skips: PartialFunction[Operator, Boolean] = { case a: HashAggOp =>
      a.skip != null
    }
    flags(options, skips) shouldContain true
    flags(options.withDisabled(Optimization.AdaptivePartialAggs), skips).contains(true) shouldBe
      false
  }

  test("pack the keys of partitioned aggregations with pack_agg_keys") {
    val packs: PartialFunction[Operator, Boolean] = { case a: PartitionedAggOp =>
      a.packKeys
    }
    flags(options, packs) shouldBe Seq(true)
    flags(options.withDisabled(Optimization.PackAggKeys), packs) shouldBe Seq(false)
  }

end EngineAdaptivePartialAggsTest
