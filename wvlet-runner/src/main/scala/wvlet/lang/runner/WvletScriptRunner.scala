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

import org.jline.terminal.Terminal
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.catalog.LazyCatalog
import wvlet.lang.runner.connector.ConnectorCatalogs
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.*
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.*
import wvlet.uni.log.LogSupport
import wvlet.uni.log.Logger

import java.io.BufferedWriter
import java.io.FilterOutputStream
import java.io.OutputStreamWriter
import java.sql.SQLException
import scala.util.control.NonFatal

case class WvletScriptRunnerConfig(
    // If true, the query result will be displayed with LESS command
    interactive: Boolean,
    resultLimit: Int = 40,
    maxColWidth: Int = 150,
    profile: Profile,
    catalog: Option[String],
    schema: Option[String],
    // Pre-compile the source folders in the background at startup. Disable for runners created
    // on demand (per-session runners on the server): the background compilation would race the
    // very statement that triggered the runner's creation within one compiler
    precompileSourcePaths: Boolean = true
)

case class LastOutput(
    line: String,
    output: String,
    result: QueryResult,
    error: Option[Throwable] = None
):
  def hasError: Boolean = error.isDefined

class WvletScriptRunner(
    workEnv: WorkEnv,
    config: WvletScriptRunnerConfig,
    queryExecutor: QueryExecutor,
    threadManager: ThreadManager
) extends AutoCloseable
    with LogSupport:

  private var units: List[CompilationUnit] = Nil

  private var resultRowLimits: Int   = config.resultLimit
  private var resultMaxColWidth: Int = config.maxColWidth

  def getResultRowLimit: Int              = resultRowLimits
  def setResultRowLimit(limit: Int): Unit = resultRowLimits = limit
  def setMaxColWidth(size: Int): Unit     = resultMaxColWidth = size

  def getCurrentCatalog: String = compiler.getDefaultCatalog.catalogName
  def getCurrentSchema: String  = compiler.getDefaultSchema

  override def close(): Unit = queryExecutor.close()

  private val compiler =
    val c = Compiler(
      CompilerOptions(
        sourceFolders = List(workEnv.path),
        workEnv = workEnv,
        catalog = config.catalog,
        schema = config.schema
      )
    )

    // Set the default catalog given in the configuration
    config
      .catalog
      .foreach { catalog =>
        c.setDefaultCatalog(
          queryExecutor
            .getDBConnector(config.profile)
            .getCatalog(catalog, config.schema.getOrElse("main"))
        )
      }
    config
      .schema
      .foreach { schema =>
        c.setDefaultSchema(schema)
      }

    // Register every profile connector by name for `from <connector>.<table>` and
    // `use <connector>` (#1861 Phase 2); LazyCatalog defers connections to first use
    config
      .profile
      .connectors
      .foreach { conn =>
        val connSchema                               = conn.schema.getOrElse("main")
        val catalogName                              = conn.catalog.getOrElse(conn.name)
        def lazyCatalogOf(name: String): LazyCatalog = LazyCatalog(
          name,
          conn.dbType,
          () =>
            ConnectorCatalogs.catalogOf(queryExecutor.getConnector(conn), conn, name, connSchema)
        )
        c.addConnectorCatalog(
          conn.name,
          lazyCatalogOf(catalogName),
          connSchema,
          // Engines may span multiple catalogs (e.g. Trino), addressable with 4-part
          // <connector>.<catalog>.<schema>.<table> names
          catalogProvider =
            if queryExecutor.isEngineConnector(conn) then
              Some(lazyCatalogOf)
            else
              None
        )
      }

    c

  end compiler

  // Warm up the compiler by pre-compiling files in the source paths while the REPL is starting
  // up. The compiler's symbol table is not thread-safe, so runStatement must join this task
  // before compiling a statement; otherwise the two compilations race and fail with spurious
  // cyclic-reference errors
  private val precompileTask: Option[java.util.concurrent.Future[?]] =
    if config.precompileSourcePaths then
      Some(
        threadManager.runBackgroundTask { () =>
          val result = compiler.compileSourcePaths(contextFile = None)
          // Report compilation errors in the initialization phases
          if result.hasFailures then
            workEnv.logError(result.failureException)
        }
      )
    else
      None

  private def awaitPrecompile(): Unit = precompileTask.foreach { task =>
    try
      task.get()
    catch
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
      // Compilation failures are already logged inside the task. Any other failure will
      // resurface when the statement itself is compiled, so only record it for debugging
      case e: Exception =>
        debug(e)
  }

  def runStatement(request: QueryRequest)(using
      queryProgressMonitor: QueryProgressMonitor
  ): QueryResult =
    val newUnit = CompilationUnit.fromWvletString(request.query)
    units = newUnit :: units

    // The compiler must not be used concurrently with the background warm-up compilation
    awaitPrecompile()
    try
      queryProgressMonitor.startCompile(newUnit)
      val compileResult = compiler.compileSingleUnit(contextUnit = newUnit)

      if !compileResult.hasFailures then
        val ctx = compileResult
          .context
          .global
          .getContextOf(newUnit)
          .withDebugRun(request.isDebugRun)
          .withQueryProgressMonitor(queryProgressMonitor)

        val queryResult = queryExecutor
          .setRowLimit(resultRowLimits)
          .executeSelectedStatement(newUnit, request.querySelection, request.linePosition, ctx)
        queryResult
      else
        ErrorResult(compileResult.failureException)
    catch
      case NonFatal(e) =>
        ErrorResult(e)
    finally
      queryProgressMonitor.close()
    end try

  end runStatement

  def displayOutput(query: String, queryResult: QueryResult, terminal: Terminal): LastOutput =
    def print: LastOutput =
      val str            = queryResult.toPrettyBox(maxColWidth = resultMaxColWidth)
      val resultMaxWidth = str.split("\n").map(_.size).max
      if !config.interactive || resultMaxWidth <= terminal.getWidth then
        // The result fits in the terminal width
        val output = queryResult.toPrettyBox(
          maxWidth = Some(terminal.getWidth),
          maxColWidth = resultMaxColWidth
        )
        if output.trim.nonEmpty then
          println(output)
      else
        // Launch less command to enable scrolling of query results in the terminal
        // TODO Use jline3's internal less
        val proc = ProcessUtil.launchInteractiveProcess("less", "-FXRSn")
        val out  =
          new BufferedWriter(
            new OutputStreamWriter(
              // Need to use a FilterOutputStream to accept keyboard events for less command along with the query result string
              new FilterOutputStream(proc.getOutputStream())
            )
          )
        out.write(str)
        out.flush()
        out.close()
        // Blocks until the process is finished
        proc.waitFor()

      LastOutput(query, str, queryResult)
    end print

    queryResult.getError match
      case None =>
        print
      case Some(e) =>
        workEnv.logError(e)
        LastOutput(query, e.getMessage, QueryResult.empty, error = Some(e))

  end displayOutput

end WvletScriptRunner
