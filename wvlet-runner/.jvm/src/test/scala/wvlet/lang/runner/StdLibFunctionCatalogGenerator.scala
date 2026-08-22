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

import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.duckdb.DuckDBConnector
import wvlet.uni.log.LogSupport

/**
  * Regenerate the DuckDB function catalog bundled in the standard library
  * (wvlet-stdlib/module/standard/duckdb/functions.wv) from the functions reported by
  * duckdb_functions() of the embedded DuckDB engine. Function names covered by the hand-written
  * standard library, the builtin typing rules, or the Wvlet syntax are skipped; the remaining
  * functions are exported as engine-native defs (`def f(...) in duckdb = native`) that compile to
  * plain SQL calls.
  *
  * Run with: ./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator"
  */
object StdLibFunctionCatalogGenerator extends LogSupport:
  def main(args: Array[String]): Unit =
    val targetPath = "wvlet-stdlib/module/standard/duckdb/functions.wv"
    val duckdb     = DuckDBConnector(WorkEnv("."))
    try
      val functions = duckdb.listFunctions("memory")
      info(s"Found ${functions.size} DuckDB functions")
      val source = StaticCatalogExporter.generateFunctionsSource(
        contextName = "duckdb",
        functions = functions,
        excludedNames = StaticCatalogExporter.handWrittenStdlibFunctionNames,
        refreshNote =
          "Re-run `./sbt \"runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator\"` to refresh."
      )
      val body = s"package wvlet.standard\n\n${source}"
      SourceIO.writeString(targetPath, body)
      info(s"Wrote ${targetPath}")
    finally
      duckdb.close()

end StdLibFunctionCatalogGenerator
