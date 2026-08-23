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
import wvlet.lang.connector.trino.TrinoConfig
import wvlet.lang.connector.trino.TrinoConnector
import wvlet.lang.runner.connector.trino.TestTrinoServer
import wvlet.uni.control.Control
import wvlet.uni.log.LogSupport

/**
  * Regenerate the Trino function catalog bundled in the standard library
  * (wvlet-stdlib/module/standard/trino/functions.wv) from the functions reported by `SHOW
  * FUNCTIONS` of an embedded Trino server (trino-testing, requires JDK 25+). Function names covered
  * by the hand-written standard library, the builtin typing rules, or the Wvlet syntax are skipped;
  * the remaining functions are exported as engine-native defs (`def f(...) in trino = native`) that
  * compile to plain SQL calls.
  *
  * Run with: ./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibTrinoFunctionCatalogGenerator"
  */
object StdLibTrinoFunctionCatalogGenerator extends LogSupport:
  /** Path of the bundled catalog, relative to the repository root */
  val targetPath = "wvlet-stdlib/module/standard/trino/functions.wv"

  /** Regeneration command shown in the catalog header and in freshness-check failures */
  val regenCommand =
    "./sbt \"runnerJVM/Test/runMain wvlet.lang.runner.StdLibTrinoFunctionCatalogGenerator\""

  /** Connect to the given embedded Trino server for reading its function catalog */
  def newTrinoConnector(server: TestTrinoServer): TrinoConnector = TrinoConnector(
    TrinoConfig(
      catalog = "memory",
      schema = "main",
      hostAndPort = server.address,
      useSSL = false,
      user = Some("test"),
      password = Some("")
    ),
    WorkEnv(".")
  )

  /** Generate the catalog source from the functions reported by the given Trino server */
  def generateCatalogSource(trino: TrinoConnector): String =
    val functions = trino.listFunctions("memory")
    info(s"Found ${functions.size} Trino functions")
    val source = StaticCatalogExporter.generateFunctionsSource(
      contextName = "trino",
      functions = functions,
      excludedNames = StaticCatalogExporter.handWrittenStdlibFunctionNames,
      refreshNote = s"Re-run `${regenCommand}` to refresh."
    )
    s"package wvlet.standard\n\n${source}"

  def main(args: Array[String]): Unit =
    val server = TestTrinoServer().withCustomMemoryPlugin
    val trino  = newTrinoConnector(server)
    try
      SourceIO.writeString(targetPath, generateCatalogSource(trino))
      info(s"Wrote ${targetPath}")
    finally
      Control.closeResources(trino, server)

end StdLibTrinoFunctionCatalogGenerator
