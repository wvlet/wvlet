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
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, DBType, WorkEnv}
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.runner.connector.trino.{TestTrinoServer, TrinoConfig, TrinoConnector}

import scala.util.control.NonFatal

/**
  * Test runner for SQL syntax tests with local Trino
  */
trait TrinoSqlSpecRunner(
    specPath: String,
    ignoredSpec: Map[String, String] = Map.empty,
    parseOnly: Boolean = false,
    prepareTPCH: Boolean = false,
    prepareTPCDS: Boolean = false
) extends AirSpec:
  
  private val testTrinoServer = TestTrinoServer().withDeltaLakePlugin
  
  override def afterAll: Unit =
    testTrinoServer.close()
    queryExecutor.close()
    dbConnectorProvider.close()
  
  private val workEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  
  private val trinoConfig = TrinoConfig(
    catalog = "memory",
    schema = "main",
    hostAndPort = testTrinoServer.address,
    useSSL = false,
    user = Some("test"),
    password = Some("")
  )
  
  private val profile = Profile(
    name = "test-trino",
    `type` = "trino",
    host = Some(testTrinoServer.address),
    catalog = Some("memory"),
    schema = Some("main"),
    user = Some("test"),
    password = Some("")
  )
    .withProperty("prepareTPCH", prepareTPCH)
    .withProperty("prepareTPCDS", prepareTPCDS)
  
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val queryExecutor = QueryExecutor(dbConnectorProvider, profile, workEnv)
  
  private val compiler = Compiler(
    CompilerOptions(
      phases =
        if parseOnly then
          Compiler.parseOnlyPhases
        else
          Compiler.allPhases,
      sourceFolders = List(specPath),
      workEnv = workEnv,
      catalog = Some("memory"),
      schema = Some("main"),
      dbType = DBType.Trino
    )
  )
  
  // Tell the compiler this is Trino
  compiler.setDefaultCatalog(
    dbConnectorProvider.getConnector(profile).getCatalog("memory", "main")
  )
  
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
    if !parseOnly then
      val result = queryExecutor.executeSingle(unit, compileResult.context.withDebugRun(true))
      debug(result.toPrettyBox(maxWidth = Some(120)))
      result
    else
      QueryResult.empty
  
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

end TrinoSqlSpecRunner

/**
  * Run TPC-H SQL syntax tests with Trino
  */
class TrinoSqlTPCHSpec extends TrinoSqlSpecRunner("spec/sql/tpc-h", parseOnly = true, prepareTPCH = true)

/**
  * Run TPC-DS SQL syntax tests with Trino
  */
class TrinoSqlTPCDSSpec extends TrinoSqlSpecRunner("spec/sql/tpc-ds", parseOnly = true, prepareTPCDS = true)

/**
  * Run basic SQL tests with Trino
  */
class TrinoSqlBasicSpec extends TrinoSqlSpecRunner("spec/sql/basic", parseOnly = true)