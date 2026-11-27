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
package wvlet.lang.engine.gpu

import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.catalog.BenchmarkKeys
import wvlet.lang.engine.EngineConformance
import wvlet.lang.engine.EngineTPCHBench
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.runtime.PlanAccelerator
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.uni.test.UniTest

import scala.collection.mutable

/**
  * Which parts of the spec/tpch plans the GPU executor would take, checked on the CPU alone (no
  * CUDA device needed): a recording accelerator accepts the subtrees [[GpuPlans.supports]] accepts
  * and lowers them on the CPU. The CPU planner changes plan shapes often, and a shape the GPU
  * doesn't know silently keeps a query on the CPU; this test names the node that blocks it.
  */
class GpuOffloadCoverageTest extends UniTest:
  private val tables      = EngineTPCHBench.tables
  private val dataDir     = sys.env.getOrElse("WVLET_GPU_TEST_DATA", "spec/tpch/data")
  private given GpuConfig = GpuConfig()

  /** The subtrees the GPU would take, and why the larger ones above them stay on the CPU */
  private class Recorder extends PlanAccelerator:
    def name: String = "gpu-coverage"
    val offloaded    = mutable.ArrayBuffer.empty[RuntimePlan]
    val blocked      = mutable.ArrayBuffer.empty[String]

    def reset(): Unit =
      offloaded.clear()
      blocked.clear()

    override def accelerate(p: RuntimePlan, cpu: RuntimePlan => Operator): Option[Operator] =
      val q = GpuPlans.inlineCTEs(p)
      if !GpuPlans.reduces(q) then
        None
      else
        GpuPlans.unsupported(q) match
          case None =>
            offloaded += q
            Some(cpu(p))
          case Some(why) =>
            blocked += why
            None

  private val recorder = Recorder()

  private val conformance = EngineConformance(
    "spec/tpch",
    tables.map(t => s"create view ${t} as select * from '${dataDir}/${t}.parquet'"),
    // With the keys the benchmark declares, which change plan shapes (q10's dependent group keys)
    engine =>
      tables.foreach(t =>
        engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet", BenchmarkKeys.of("tpch", t))
      ),
    accelerator = Some(recorder)
  )

  override def afterAll: Unit = conformance.close()

  /** The tables a plan scans, with repeats */
  private def scanned(p: RuntimePlan): Seq[String] = GpuPlans.scansOf(p).map(_.source.toString)

  test("offload the joins and aggregations of every TPC-H query") {
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("spec/tpch"), workEnv = WorkEnv("spec/tpch"))
    )
    val units = compiler
      .localCompilationUnits
      .filter(_.sourceFile.fileName.matches("q\\d+\\.wv"))
      .sortBy(u => EngineTPCHBench.queryNumber(u.sourceFile.fileName))
    val report = units.flatMap { unit =>
      val ctx       = compiler.compileSingleUnit(unit).context
      given Context = ctx
      EngineTPCHBench
        .queryOf(unit)
        .map { q =>
          recorder.reset()
          conformance.engine.explain(q)
          if sys.env.contains("WVLET_GPU_COVERAGE_PLANS") then
            recorder.offloaded.foreach(p => info(s"  offloaded:\n${p.toString.take(3000)}"))
          val name = unit.sourceFile.fileName.stripSuffix(".wv")
          info(
            s"${name}: ${recorder.offloaded.size} GPU subtree(s) over ${recorder
                .offloaded
                .flatMap(scanned)
                .size} scan(s); CPU: ${recorder.blocked.distinct.mkString("; ")}"
          )
          (name, recorder.offloaded.nonEmpty, recorder.blocked.distinct.toList)
        }
    }
    report.size shouldBe 22
    // Every query offloads something, and only these known shapes keep a part of one on the CPU
    report.filterNot(_._2).map(_._1) shouldBe Nil
    val unexpected = report.flatMap((name, _, blocked) =>
      blocked.filterNot(knownBlockers.getOrElse(name, Set.empty)).map(why => s"${name}: ${why}")
    )
    unexpected shouldBe Nil
  }

  // The GPU executor runs no window functions (q2's and q17's correlated min and avg, and a part of
  // q16 with declared keys, become windows)
  // and no cross joins (q2's join with its plan-time minimum)
  private val knownBlockers = Map(
    "q2"  -> Set("Join: join kind Cross", "Window"),
    "q16" -> Set("Window"),
    "q17" -> Set("Window")
  )

end GpuOffloadCoverageTest
