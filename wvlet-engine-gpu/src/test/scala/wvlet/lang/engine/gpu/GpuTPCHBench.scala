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

import wvlet.lang.engine.EngineTPCHBench
import wvlet.uni.test.UniTest

/**
  * EngineTPCHBench with the GPU executor: the engine offloads the plan subtrees the GPU supports.
  * Ignored unless WVLET_ENGINE_BENCH_SF is set and a CUDA device is present. The same variables
  * apply; in particular `WVLET_ENGINE_BENCH_AB=gpu_offload` compares the engine with and without
  * the GPU in one JVM, and without it the engine (with GPU) runs against DuckDB.
  *
  * {{{
  *   WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_AB=gpu_offload ./sbt "engineGpu/testOnly *GpuTPCHBench"
  * }}}
  */
class GpuTPCHBench extends UniTest:
  import EngineTPCHBench.*

  test("time spec/tpch queries with GPU offload") {
    BenchOptions.fromEnv() match
      case None =>
        ignore("Set WVLET_ENGINE_BENCH_SF=<scale factor> (e.g. 10) to run the GPU benchmark")
      case Some(_) if !GpuRuntime.isAvailable =>
        ignore("No CUDA device")
      case Some(opts) =>
        val dataDir = prepareData(opts.sf)
        runBench("tpch-gpu", "spec/tpch", dataDir, tables, opts, Some(GpuAccelerator()))
  }
