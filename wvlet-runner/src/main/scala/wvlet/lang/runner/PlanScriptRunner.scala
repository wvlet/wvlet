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

import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * The REPL's [[ScriptRunner]] on every platform (JVM, Node.js, Native): one long-lived compiler
  * and [[PlanExecutor]] over the cross-platform engines (the in-process wvlet engine, DuckDB, and
  * Trino), so `use` and saved tables persist across statements.
  *
  * On the in-process engine, statements compile against the engine session's catalog, so tables
  * saved by earlier statements are typed. Other engines compile with the profile's dialect without
  * reading live metadata, like `wvlet run` of the npm CLI.
  *
  * @param interactive
  *   page results wider than the terminal with `pager` instead of cutting them
  * @param pager
  *   shows a text with a pager; returns false when no pager could be launched
  */
class PlanScriptRunner(
    workEnv: WorkEnv,
    profile: Profile,
    interactive: Boolean,
    resultLimit: Int = 40,
    maxColWidth: Int = 150,
    pager: String => Boolean = _ => false
) extends ScriptRunner
    with LogSupport:

  private var resultRowLimit: Int    = resultLimit
  private var resultMaxColWidth: Int = maxColWidth

  override def getResultRowLimit: Int              = resultRowLimit
  override def setResultRowLimit(limit: Int): Unit = resultRowLimit = limit
  override def setMaxColWidth(size: Int): Unit     = resultMaxColWidth = size

  private val engine            = profile.defaultEngine
  private val connectorProvider = SqlConnectorProvider(profile)

  // The session of the in-process engine, shared by the compiler (its catalog) and the executor
  private val engineSession: Option[WvletEngineSession] =
    Option.when(WvletEngineSession.isInProcess(engine))(WvletEngineSession(engine, workEnv.path))

  private val executor = PlanExecutor(
    connectorProvider,
    profile,
    workEnv,
    rowLimit = resultLimit,
    initialEngineSessions = engineSession.map(engine -> _).toMap
  )

  private val compiler =
    val c = Compiler(
      CompilerOptions(
        sourceFolders = List(workEnv.path),
        workEnv = workEnv,
        catalog = engine.catalog,
        schema = engine.schema,
        dbType = engine.dbType
      )
    )
    engineSession.foreach { session =>
      c.setDefaultCatalog(session.catalog)
      c.setDefaultSchema(engine.schema.getOrElse(PlanScriptRunner.engineSchema))
    }
    c

  override def getCurrentCatalog: String = compiler.getDefaultCatalog.catalogName
  override def getCurrentSchema: String  = compiler.getDefaultSchema

  // Compile the .wv files of the work folder before the first statement, so it can reference
  // their models. Compilation errors are reported once, without stopping the REPL
  private lazy val precompile: Unit =
    try
      val result = compiler.compileSourcePaths(contextFile = None)
      if result.hasFailures then
        workEnv.logError(result.failureException)
    catch
      case NonFatal(e) =>
        debug(e)

  override def close(): Unit =
    executor.close()
    connectorProvider.close()

  override def runStatement(request: QueryRequest)(using
      queryProgressMonitor: QueryProgressMonitor
  ): QueryResult =
    precompile
    val newUnit = CompilationUnit.fromWvletString(request.query)
    try
      queryProgressMonitor.startCompile(newUnit)
      val compileResult = compiler.compileSingleUnit(contextUnit = newUnit)
      if compileResult.hasFailures then
        ErrorResult(compileResult.failureException)
      else
        val ctx = compileResult
          .context
          .global
          .getContextOf(newUnit)
          .withDebugRun(request.isDebugRun)
          .withQueryProgressMonitor(queryProgressMonitor)
        executor
          // A request-level maxRows overrides the interactive row limit
          .setRowLimit(request.maxRows.getOrElse(resultRowLimit))
          .executeSelectedStatement(newUnit, request.querySelection, request.linePosition, ctx)
    catch
      case NonFatal(e) =>
        ErrorResult(e)
    finally
      queryProgressMonitor.close()
    end try

  end runStatement

  override def displayOutput(
      query: String,
      queryResult: QueryResult,
      terminalWidth: Int
  ): LastOutput =
    queryResult.getError match
      case None =>
        val str      = queryResult.toPrettyBox(maxColWidth = resultMaxColWidth)
        val maxWidth = str.split("\n").map(_.length).maxOption.getOrElse(0)
        // Page a result wider than the terminal, or cut it to the terminal width
        if !(interactive && maxWidth > terminalWidth && pager(str)) then
          val output = queryResult.toPrettyBox(
            maxWidth = Some(terminalWidth),
            maxColWidth = resultMaxColWidth
          )
          if output.trim.nonEmpty then
            println(output)
        LastOutput(query, str, queryResult)
      case Some(e) =>
        workEnv.logError(e)
        LastOutput(query, e.getMessage, QueryResult.empty, error = Some(e))

  override def renderOutput(query: String, queryResult: QueryResult, maxWidth: Int): LastOutput =
    queryResult.getError match
      case None =>
        val str = queryResult.toPrettyBox(
          maxWidth = Some(maxWidth),
          maxColWidth = resultMaxColWidth
        )
        LastOutput(query, str, queryResult)
      case Some(e) =>
        workEnv.logError(e)
        LastOutput(query, e.getMessage, QueryResult.empty, error = Some(e))

end PlanScriptRunner

object PlanScriptRunner:
  /** The schema of the in-process engine's tables */
  val engineSchema: String = "main"
