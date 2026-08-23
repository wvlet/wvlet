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
  /** Path of the bundled catalog, relative to the repository root */
  val targetPath = "wvlet-stdlib/module/standard/duckdb/functions.wv"

  /** Regeneration command shown in the catalog header and in freshness-check failures */
  val regenCommand =
    "./sbt \"runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator\""

  /** Generate the catalog source from the functions reported by the given DuckDB engine */
  def generateCatalogSource(duckdb: DuckDBConnector): String =
    val functions = duckdb.listFunctions("memory")
    info(s"Found ${functions.size} DuckDB functions")
    val source = StaticCatalogExporter.generateFunctionsSource(
      contextName = "duckdb",
      functions = functions,
      excludedNames = StaticCatalogExporter.handWrittenStdlibFunctionNames,
      refreshNote = s"Re-run `${regenCommand}` to refresh."
    )
    s"package wvlet.standard\n\n${source}"

  def main(args: Array[String]): Unit =
    val duckdb = DuckDBConnector(WorkEnv("."))
    try
      SourceIO.writeString(targetPath, generateCatalogSource(duckdb))
      info(s"Wrote ${targetPath}")
    finally
      duckdb.close()

end StdLibFunctionCatalogGenerator
