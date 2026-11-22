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

import wvlet.uni.test.UniTest

/**
  * Opt-in benchmark of the columnar engine against DuckDB on the TPC-DS queries in spec/tpcds, both
  * reading the same Parquet files generated with DuckDB's tpcds extension into
  * `target/tpcds-sf<sf>/`. Timings are logged and written to
  * `target/engine-bench/tpcds-sf<sf>.tsv`.
  *
  * {{{
  *   WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCDSBench"
  * }}}
  *
  * It takes the same environment variables as [[EngineTPCHBench]].
  */
class EngineTPCDSBench extends UniTest:
  import EngineTPCHBench.*

  test("time spec/tpcds queries on the engine and DuckDB") {
    BenchOptions.fromEnv() match
      case None =>
        ignore("Set WVLET_ENGINE_BENCH_SF=<scale factor> (e.g. 1) to run the engine benchmark")
      case Some(opts) =>
        val (dataDir, tables) = EngineTPCDSTest.prepareData(opts.sf)
        runBench("tpcds", "spec/tpcds", dataDir, tables, opts)
  }

end EngineTPCDSBench
