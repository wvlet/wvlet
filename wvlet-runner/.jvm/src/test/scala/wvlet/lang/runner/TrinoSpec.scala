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

import wvlet.uni.test.UniTest
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.uni.control.Control
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.connector.trino.TestTrinoServer

import scala.util.control.NonFatal

/**
  * Runs the .wv specs of a folder through a database connector, one test per file. Subclasses build
  * the connection, then call [[runSpecs]] from their constructor
  */
trait ConnectorSpecRunner(specPath: String, specFilter: String => Boolean) extends UniTest:
  protected val workEnv: WorkEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private var resources: Seq[AutoCloseable] = Nil

  override def afterAll: Unit =
    // Close in reverse creation order
    Control.closeResources(resources*)

  /**
    * Register a test for each spec file matching specFilter, compiled for and run on the profile's
    * connector. `owned` are closed after the tests, after the executor and connectors
    */
  protected def runSpecs(
      profile: Profile,
      catalog: String,
      schema: String,
      owned: AutoCloseable*
  ): Unit =
    val dbConnectorProvider = ConnectorProvider(workEnv)
    val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)
    resources = Seq(queryExecutor, dbConnectorProvider) ++ owned
    val compiler = Compiler(
      CompilerOptions(
        sourceFolders = List(specPath),
        workEnv = workEnv,
        catalog = Some(catalog),
        schema = Some(schema)
      )
    )
    // Ensure the compiler knows the target database
    compiler.setDefaultCatalog(
      dbConnectorProvider.getConnector(profile).getCatalog(catalog, schema)
    )

    for
      unit <- compiler.localCompilationUnits
      if specFilter(unit.sourceFile.fileName)
    do
      test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
        try
          trace(s"compiling: ${unit.sourceFile}")
          val compileResult = compiler.compileSingleUnit(unit)
          trace(s"compiled: ${unit.sourceFile}")
          val result = queryExecutor.executeSingle(unit, compileResult.context.withDebugRun(true))
          debug(result.toPrettyBox(maxWidth = Some(120)))
          handleResult(result)
        catch
          case NonFatal(e) =>
            handleError(e)
      }

  end runSpecs

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

end ConnectorSpecRunner

abstract class TrinoSpecRunner(specPath: String, specFilter: String => Boolean = _ => true)
    extends ConnectorSpecRunner(specPath, specFilter):

  // Launch embedded Trino with an in-memory catalog
  private val server = TestTrinoServer().withCustomMemoryPlugin
  runSpecs(
    Profile(
      name = s"local-trino@${server.address}",
      connectors = Seq(
        ConnectorConfig(
          name = "trino",
          `type` = "trino",
          user = Some("test"),
          password = Some(""),
          host = Some(server.address),
          catalog = Some("memory"),
          schema = Some("main"),
          properties = Map("useSSL" -> false)
        )
      )
    ),
    catalog = "memory",
    schema = "main",
    server
  )

end TrinoSpecRunner

/**
  * Specs evaluated with a local Trino server
  */
class LocalTrinoSpec extends TrinoSpecRunner("spec/trino")

/**
  * Run the standard-library specs (spec/basic/stdlib_*.wv) against a real Trino server so the `in
  * trino` SQL mappings are verified by execution, not only by generated-SQL comparison. The specs
  * use only literal tables, so no fixture data is needed.
  */
class TrinoStdLibSpec extends TrinoSpecRunner("spec/basic", _.startsWith("stdlib_"))
