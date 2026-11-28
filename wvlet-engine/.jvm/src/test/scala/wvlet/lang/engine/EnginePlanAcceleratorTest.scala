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

import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.runtime.PlanAccelerator
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.Schema
import wvlet.uni.test.UniTest

/**
  * The [[PlanAccelerator]] hook: Lowering offers plan subtrees to the accelerator top-down, and the
  * `gpu_offload` optimization turns that off. A stub accelerator takes aggregations and runs them
  * through the CPU lowering it is given, so the results stay the CPU engine's.
  */
class EnginePlanAcceleratorTest extends UniTest:
  private class Stub(recomputes: Boolean = false) extends PlanAccelerator:
    var offered                                                                    = 0
    def name                                                                       = "stub"
    override def recomputesSharedCTEs: Boolean                                     = recomputes
    def accelerate(p: RuntimePlan, cpu: RuntimePlan => Operator): Option[Operator] =
      offered += 1
      p match
        case _: RuntimePlan.Aggregate =>
          Some(StubOp(cpu(p)))
        case _ =>
          None

  private class StubOp(inner: Operator) extends Operator:
    def schema: Schema                   = inner.schema
    def next(): Batch                    = inner.next()
    override def children: Seq[Operator] = Seq(inner)
    override def name: String            = "Stub"

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List("spec/tpch"), workEnv = WorkEnv("spec/tpch"))
  )

  private def withQ6(f: Context ?=> wvlet.lang.model.plan.Relation => Unit): Unit =
    withQuery("q6")(f)

  private def withQuery(name: String)(f: Context ?=> wvlet.lang.model.plan.Relation => Unit): Unit =
    val unit      = compiler.localCompilationUnits.find(_.sourceFile.fileName == s"${name}.wv").get
    val ctx       = compiler.compileSingleUnit(unit).context
    given Context = ctx
    f(EngineTPCHBench.queryOf(unit).get)

  private def engine(): WvletEngine =
    val e = WvletEngine(baseDir = "spec/tpch")
    EngineTPCHBench
      .tables
      .foreach(t => e.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))
    e

  test("run the subtrees an accelerator takes as its operators") {
    withQ6 { q =>
      val cpu   = engine()
      val stub  = Stub()
      val accel = cpu.withAccelerator(stub)
      accel.explain(q) shouldContain "Stub"
      stub.offered shouldMatch {
        case n: Int if n > 0 =>
      }
      accel.execute(q).rows.toList shouldBe cpu.execute(q).rows.toList
    }
  }

  test("offer no plan to the accelerator with gpu_offload disabled") {
    withQ6 { q =>
      val stub = Stub()
      val off  = engine()
        .withAccelerator(stub)
        .withOptimizer(plan.OptimizerOptions().withDisabled(Optimization.GpuOffload))
      off.explain(q).contains("Stub") shouldBe false
      stub.offered shouldBe 0
    }
  }

  test("skip the plan-time CTE replan for an accelerator that recomputes shared CTEs") {
    // q15's two revenue subqueries share a filtered lineitem scan that joins read: by default the
    // planner computes it at plan time and lowers the re-planned query again
    withQuery("q15") { q =>
      val replanning  = Stub()
      val recomputing = Stub(recomputes = true)
      val expected    = engine().withAccelerator(replanning).execute(q).rows.toList
      val offeredBy   = replanning.offered
      engine().withAccelerator(recomputing).execute(q).rows.toList shouldBe expected
      // Lowered once instead of twice (counts in the failure message)
      (recomputing.offered, offeredBy) shouldMatch {
        case (n: Int, m: Int) if n < m =>
      }
      // The skip follows gpu_offload: with it off, the accelerator gets no plan and the query is
      // re-planned as on the CPU
      val off = Stub(recomputes = true)
      engine()
        .withAccelerator(off)
        .withOptimizer(plan.OptimizerOptions().withDisabled(Optimization.GpuOffload))
        .execute(q)
        .rows
        .toList shouldBe expected
      off.offered shouldBe 0
    }
  }

end EnginePlanAcceleratorTest
