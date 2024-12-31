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

trait SpecRunner(
    specPath: String,
    ignoredSpec: Map[String, String] = Map.empty,
    prepareTPCH: Boolean = false
) extends AirSpec:
  private val workEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private val profile = Profile.defaultDuckDBProfile.withProperty("prepareTPCH", prepareTPCH)
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)
  override def afterAll: Unit =
    queryExecutor.close()
    dbConnectorProvider.close()

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(specPath), workEnv = workEnv)
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
    val compileResult = compiler.compileSingleUnit(unit)
    val result        = queryExecutor.executeSingle(unit, compileResult.context.withDebugRun(true))
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

end SpecRunner

class BasicSpec
    extends SpecRunner(
      "spec/basic",
      ignoredSpec = Map("values.wv" -> "Need to support array[struct] data")
    )

class TPCHSpec extends SpecRunner("spec/tpch", prepareTPCH = true)

// Negative tests, expecting some errors
class NegSpec extends SpecRunner("spec/neg"):
  override protected def handleError: Throwable => Unit =
    case e: WvletLangException if e.statusCode.isUserError =>
      // Expected error
      debug(e)
    case e: Throwable =>
      throw e

class CDPBehaviorSpec extends SpecRunner("spec/cdp_behavior")
