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
import wvlet.lang.runner.connector.trino.TestTrinoServer

import scala.util.control.NonFatal

trait TrinoSpecRunner(specPath: String) extends AirSpec:

  // Launch embedded Trino with an in-memory catalog
  private val server  = TestTrinoServer().withCustomMemoryPlugin
  private val workEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private val profile = Profile(
    name = s"local-trino@${server.address}",
    `type` = "trino",
    user = Some("test"),
    password = Some(""),
    host = Some(server.address),
    catalog = Some("memory"),
    schema = Some("main"),
    properties = Map("useSSL" -> false)
  )

  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)

  override def afterAll: Unit =
    try queryExecutor.close()
    finally
      try dbConnectorProvider.close()
      finally server.close()

  private val compiler = Compiler(
    CompilerOptions(
      sourceFolders = List(specPath),
      workEnv = workEnv,
      catalog = Some("memory"),
      schema = Some("main")
    )
  )

  // Ensure the compiler knows we are targeting Trino
  compiler.setDefaultCatalog(dbConnectorProvider.getConnector(profile).getCatalog("memory", "main"))

  private def isLocalSpec(u: CompilationUnit): Boolean =
    val f = u.sourceFile.fileName
    f.startsWith("local-") || f.startsWith("local_")

  // Compile .wv files and run only local-* specs in the folder
  for unit <- compiler.localCompilationUnits.filter(isLocalSpec) do
    test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
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

end TrinoSpecRunner

class LocalTrinoSpec extends TrinoSpecRunner("spec/trino")
