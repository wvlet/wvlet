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

import wvlet.airspec.AirSpec
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import scala.util.control.NonFatal

trait RunnerSpec(
    specPath: String,
    ignoredSpec: Map[String, String] = Map.empty,
    parseOnly: Boolean = false,
    prepareTPCH: Boolean = false,
    prepareTPCDS: Boolean = false
) extends AirSpec:
  private val workEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private val profile = Profile
    .defaultDuckDBProfile
    .withProperty("prepareTPCH", prepareTPCH)
    .withProperty("prepareTPCDS", prepareTPCDS)

  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)
  override def afterAll: Unit =
    queryExecutor.close()
    dbConnectorProvider.close()

  private val compiler = Compiler(
    CompilerOptions(
      phases =
        if parseOnly then
          Compiler.parseOnlyPhases
        else
          Compiler.allPhases
      ,
      sourceFolders = List(specPath),
      workEnv = workEnv
    )
  )

  compiler.setDefaultCatalog(queryExecutor.getDBConnector(profile).getCatalog("memory", "main"))

  // Compile all files in the source paths first
  for unit <- compiler.localCompilationUnits do
    test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))
      try
        handleResult(runSpec(unit))
      catch
        case NonFatal(e) =>
          handleError(e)
    }

  protected def runSpec(unit: CompilationUnit): QueryResult =
    trace(s"compiling: ${unit.sourceFile}")
    val compileResult = compiler.compileSingleUnit(unit)
    trace(s"compiled: ${unit.sourceFile}")
    val result = queryExecutor.executeSingle(unit, compileResult.context.withDebugRun(true))
    debug(result.toPrettyBox(maxWidth = Some(120)))
    result

  protected def handleResult(result: QueryResult): Unit =
    result
      .getWarning
      .foreach { w =>
        warn(w)
      }
    result.getError match
      case Some(e) =>
        throw e
      case None =>
      // ok

  protected def handleError: Throwable => Unit =
    case e: WvletLangException if e.statusCode.isUserError =>
      workEnv.errorLogger.error(e)
      fail(e.getMessage)
    case e: Throwable =>
      throw e

end RunnerSpec

class RunnerSpecBasic
    extends RunnerSpec(
      "spec/basic",
      ignoredSpec = Map("table-value-constant.wv" -> "Need to support table value constant")
    )

class RunnerSpecTPCH extends RunnerSpec("spec/tpch", prepareTPCH = true)

// Negative tests, expecting some errors
class RunnerSpecNeg extends RunnerSpec("spec/neg"):
  override protected def handleError: Throwable => Unit =
    case e: WvletLangException if e.statusCode.isUserError =>
      // Expected error
      debug(e)
    case e: Throwable =>
      throw e

class RunnerSpecCDPBehavior
    extends RunnerSpec(
      "spec/cdp_behavior",
      ignoredSpec = Map("behavior.wv" -> "Need to support subscribe")
    )

class RunnerSpecSqlBasic
    extends RunnerSpec(
      "spec/sql/basic",
      ignoredSpec = Map(
        "show-create-view.sql" -> "SHOW CREATE VIEW execution not yet fully supported",
        "date-time-function-calls.sql" ->
          "Testing parser for date/time/timestamp functions - DuckDB lacks these functions",
        "nested-parentheses-tablesample.sql" -> "Handle engine specific samplign percentage",
        "tablesample.sql"                    -> "Handle engine specific samplign percentage"
      )
    )

class RunnerSpecSqlTPCH extends RunnerSpec("spec/sql/tpc-h", parseOnly = true, prepareTPCH = true)

class RunnerSpecSqlTPCDS
    extends RunnerSpec("spec/sql/tpc-ds", parseOnly = true, prepareTPCDS = true)
