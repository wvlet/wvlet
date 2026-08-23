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

import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.TestTrinoServer
import wvlet.uni.control.Control
import wvlet.uni.test.UniTest

/**
  * Guard against drift between the engine-native function catalogs bundled in the standard library
  * and the functions actually reported by the embedded engines. When an engine dependency (DuckDB
  * JDBC driver, Trino testing server) is upgraded and its function set changes, this test fails
  * with the command to regenerate the bundled catalog.
  */
class StdLibCatalogFreshnessTest extends UniTest:

  private def assertCatalogIsFresh(
      targetPath: String,
      generated: String,
      regenCommand: String
  ): Unit =
    val bundled = SourceIO.readAsString(targetPath)
    if bundled != generated then
      val bundledLines   = bundled.linesIterator.toIndexedSeq
      val generatedLines = generated.linesIterator.toIndexedSeq
      val firstDiff      = bundledLines
        .zipAll(generatedLines, "<missing line>", "<missing line>")
        .indexWhere(_ != _)
      val diffReport =
        if firstDiff >= 0 then
          val expected = generatedLines.lift(firstDiff).getOrElse("<missing line>")
          val actual   = bundledLines.lift(firstDiff).getOrElse("<missing line>")
          s"First difference at line ${firstDiff +
              1}:\n  bundled  : ${actual}\n  generated: ${expected}"
        else
          ""
      fail(
        s"""${targetPath} is out of date (bundled: ${bundledLines
            .size} lines, regenerated: ${generatedLines.size} lines).
           |${diffReport}
           |Regenerate it with:
           |  ${regenCommand}""".stripMargin
      )

  test("bundled DuckDB function catalog matches the embedded engine") {
    val duckdb = DuckDBConnector(WorkEnv("."))
    try assertCatalogIsFresh(
        StdLibFunctionCatalogGenerator.targetPath,
        StdLibFunctionCatalogGenerator.generateCatalogSource(duckdb),
        StdLibFunctionCatalogGenerator.regenCommand
      )
    finally duckdb.close()
  }

  test("bundled Trino function catalog matches the embedded server") {
    val server = TestTrinoServer().withCustomMemoryPlugin
    val trino  = StdLibTrinoFunctionCatalogGenerator.newTrinoConnector(server)
    try assertCatalogIsFresh(
        StdLibTrinoFunctionCatalogGenerator.targetPath,
        StdLibTrinoFunctionCatalogGenerator.generateCatalogSource(trino),
        StdLibTrinoFunctionCatalogGenerator.regenCommand
      )
    finally Control.closeResources(trino, server)
  }

end StdLibCatalogFreshnessTest
