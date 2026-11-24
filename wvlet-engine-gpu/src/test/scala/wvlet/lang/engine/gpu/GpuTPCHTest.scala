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
import wvlet.lang.engine.EngineConformance
import wvlet.lang.engine.EngineTPCHBench
import wvlet.uni.test.UniTest

/**
  * Runs spec/tpch on the engine with GPU offload and checks the rows against DuckDB, as
  * EngineTPCHTest does for the CPU engine. Skipped without a CUDA device. `WVLET_GPU_TEST_DATA`
  * names another folder of TPC-H Parquet files (e.g. `target/tpch-sf10`), to check values at scale.
  */
class GpuTPCHTest extends UniTest:
  private val tables  = EngineTPCHBench.tables
  private val dataDir = sys.env.getOrElse("WVLET_GPU_TEST_DATA", "spec/tpch/data")

  // Neither touches the GPU before a query runs: the device is initialized on first use
  private val accelerator = GpuAccelerator()

  private val conformance = EngineConformance(
    "spec/tpch",
    tables.map(t => s"create view ${t} as select * from '${dataDir}/${t}.parquet'"),
    engine => tables.foreach(t => engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet")),
    accelerator = Some(accelerator)
  )

  override def afterAll: Unit = conformance.close()

  private def requireGpu(): Unit =
    if !GpuRuntime.isAvailable then
      ignore("No CUDA device")

  test("offload TPC-H plans to the GPU") {
    requireGpu()
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("spec/tpch"), workEnv = WorkEnv("spec/tpch"))
    )
    val units = compiler
      .localCompilationUnits
      .filter(_.sourceFile.fileName.matches("q\\d+\\.wv"))
      .sortBy(u => EngineTPCHBench.queryNumber(u.sourceFile.fileName))
    val gpuQueries = units.flatMap { unit =>
      // Compile first: a `given` alias is initialized lazily, after queryOf reads the plan
      val ctx       = compiler.compileSingleUnit(unit).context
      given Context = ctx
      EngineTPCHBench
        .queryOf(unit)
        .map { q =>
          val plan  = conformance.engine.explain(q)
          val lines = plan.linesIterator.filter(_.contains("Gpu[")).toList
          val name  = unit.sourceFile.fileName.stripSuffix(".wv")
          info(
            s"${name}: ${lines.size} GPU subtree(s)${lines
                .map(l => s"\n  ${l.trim.take(160)}")
                .mkString}"
          )
          (name, lines.nonEmpty)
        }
    }
    // A single-table scan, filter, and aggregation runs on the GPU as a whole
    gpuQueries.toMap.get("q1") shouldBe Some(true)
    gpuQueries.toMap.get("q6") shouldBe Some(true)
  }

  for unit <- conformance.units do
    test(s"${unit.sourceFile.fileName} with GPU offload") {
      requireGpu()
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }

end GpuTPCHTest
