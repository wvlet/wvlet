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
package wvlet.lang.runner

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

/**
  * Opt-in timing probe for the TPC-H queries in spec/tpch on DuckDB, in the spirit of TyperBench:
  * no assertion, the numbers are logged for before/after comparison on one machine.
  *
  * Run with the scale factor in the environment (sbt forks the test JVM, so a -D property would not
  * reach it):
  * {{{
  *   WVLET_TPCH_BENCH_SF=1 ./sbt "runnerJVM/testOnly *TPCHBench"
  * }}}
  * `WVLET_TPCH_BENCH_RUNS` (default 3) sets the number of timed executions per query after one
  * warm-up run. Without `WVLET_TPCH_BENCH_SF` the probe is ignored, so it never runs in CI.
  */
class TPCHBench extends UniTest:

  test("time spec/tpch queries on DuckDB") {
    sys.env.get("WVLET_TPCH_BENCH_SF").map(_.toDouble) match
      case None =>
        ignore("Set WVLET_TPCH_BENCH_SF=<scale factor> (e.g. 1) to run the TPC-H timing probe")
      case Some(sf) =>
        runBench(sf, runs = sys.env.get("WVLET_TPCH_BENCH_RUNS").map(_.toInt).getOrElse(3))
  }

  private def runBench(sf: Double, runs: Int): Unit =
    val specPath = "spec/tpch"
    val workEnv  = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
    val profile  = Profile
      .defaultDuckDBProfile
      .withProperty("prepareTPCH", true)
      .withProperty("tpchScaleFactor", sf)

    val connectorProvider = ConnectorProvider(workEnv)
    val queryExecutor     = QueryExecutor(connectorProvider, profile, workEnv)
    try
      def millisSince(nano: Long): Double = (System.nanoTime() - nano) / 1000000.0

      // Bootstrapping DuckDB (native library load) and generating the data happen in the
      // connector's background thread; the first statement waits for them
      val setupStart             = System.nanoTime()
      val connector              = queryExecutor.getDBConnector(profile)
      given QueryProgressMonitor = QueryProgressMonitor.noOp
      connector.execute("select 1")
      info(f"DuckDB setup with dbgen(sf=${sf}): ${millisSince(setupStart)}%.1f ms")

      val compiler = Compiler(CompilerOptions(sourceFolders = List(specPath), workEnv = workEnv))
      compiler.setDefaultCatalog(connector.getCatalog("memory", "main"))

      val units = compiler
        .localCompilationUnits
        .filter(_.sourceFile.fileName.matches("q\\d+\\.wv"))
        .sortBy(_.sourceFile.fileName.stripPrefix("q").stripSuffix(".wv").toInt)

      var totalMedian = 0.0
      for unit <- units do
        val compileStart  = System.nanoTime()
        val compileResult = compiler.compileSingleUnit(unit)
        val compileMs     = millisSince(compileStart)
        // Plain execution: the query files carry no test assertions, and the sf=0.01
        // expectations in spec/tpch/test would not hold at other scale factors anyway
        val ctx                   = compileResult.context.withDebugRun(false)
        def executeOnce(): Double =
          val start = System.nanoTime()
          queryExecutor.executeSingle(unit, ctx)
          millisSince(start)
        executeOnce() // warm-up
        val times  = (1 to runs).map(_ => executeOnce()).sorted
        val median = times(times.size / 2)
        totalMedian += median
        info(
          f"${unit.sourceFile.fileName}%-8s compile ${compileMs}%7.1f ms  exec min ${times
              .head}%8.1f ms  median ${median}%8.1f ms  max ${times.last}%8.1f ms"
        )
      info(f"sum of per-query median exec time over ${units.size} queries: ${totalMedian}%.1f ms")
    finally
      queryExecutor.close()
      connectorProvider.close()
    end try

  end runBench

end TPCHBench
