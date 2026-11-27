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
package wvlet.lang.embedded

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.PlanResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.QueryResultList
import wvlet.lang.runner.TableRows
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogLevel

/**
  * Wvlet's columnar query engine embedded in the application process, the way an app embeds DuckDB
  * or SQLite: register tables from files, then run Wvlet queries on them with no server, no
  * database, and no SQL generation.
  *
  * {{{
  * val engine = EmbeddedEngine.open("data")
  * engine.registerTable("orders", "orders.parquet")
  * val result = engine.query("from orders group by o_orderstatus agg _.count as n")
  * result.rows.foreach(println)
  * engine.close()
  * }}}
  *
  * An engine is one session: tables written by `save to` or `create table` stay visible to later
  * queries. Calls on one engine run one at a time; open an engine per thread to run queries in
  * parallel.
  *
  * @param baseDir
  *   the folder that relative file paths, in [[registerTable]] and in queries (`from 'x.parquet'`),
  *   resolve against
  */
class EmbeddedEngine private (val baseDir: String) extends AutoCloseable:
  private val config  = ConnectorConfig(name = "wvlet", `type` = "wvlet", default = true)
  private val profile = Profile("embedded", List(config))
  private val workEnv = WorkEnv(baseDir, logLevel = LogLevel.WARN)
  private val session = WvletEngineSession(config, baseDir)

  // Compiles against the session's catalog, so registered Parquet tables are typed at compile time
  private val compiler =
    val c = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = workEnv, dbType = DBType.Wvlet))
    c.setDefaultCatalog(session.catalog)
    c.setDefaultSchema(EmbeddedEngine.schemaName)
    c

  private val connectorProvider = SqlConnectorProvider(profile)
  private val executor          = PlanExecutor(
    connectorProvider,
    profile,
    workEnv,
    rowLimit = Int.MaxValue,
    initialEngineSessions = Map(config -> session)
  )

  private var closed = false

  private def checkOpen(): Unit =
    if closed then
      throw StatusCode.INVALID_ARGUMENT.newException("The embedded engine is already closed")

  /**
    * Register a table read from a file or folder: Parquet (a file, a glob like `*.parquet`, or a
    * folder of files, Hive-partitioned or not), an Iceberg table's `metadata.json` or folder, JSON
    * Lines, or CSV / TSV, optionally gzip-compressed. A relative path resolves against `baseDir`.
    * Registering a name again replaces the table.
    */
  def registerTable(name: String, path: String): Unit = synchronized {
    checkOpen()
    val resolved =
      if SourceIO.isAbsolutePath(path) || path.contains("://") then
        path
      else
        s"${baseDir.stripSuffix("/")}/${path}"
    val source = WvletEngineSession
      .sourceOf(name, resolved, SourceIO.isDirectory(resolved))
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Unsupported data file for table ${name}: ${path}")
      )
    session.register(source)
  }

  /** Names of the registered tables, including tables saved by queries */
  def tableNames: Seq[String] = synchronized(session.tableNames)

  /**
    * Compile and run Wvlet statements. Statements run in order (`save to`, `create table`, and
    * `test` included), and the rows of the last query are returned; statements that return no rows
    * give an empty result.
    *
    * @throws WvletLangException
    *   when the query fails to compile, a statement fails, or a `test` assertion fails. The
    *   exception carries a status code and the source location
    */
  def query(text: String): EmbeddedResult = synchronized {
    checkOpen()
    val unit          = CompilationUnit.fromWvletString(text)
    val compileResult = compiler.compileSingleUnit(unit)
    if compileResult.hasFailures then
      throw compileResult.failureException
    val ctx = compileResult
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val result = executor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
    EmbeddedEngine.firstError(result).foreach(throw _)
    EmbeddedEngine
      .lastTable(result)
      .map(EmbeddedResult.fromTableRows)
      .getOrElse(EmbeddedResult.empty)
  }

  /** Drop the engine's tables. Later calls fail */
  override def close(): Unit = synchronized {
    if !closed then
      closed = true
      executor.close()
      connectorProvider.close()
  }

end EmbeddedEngine

object EmbeddedEngine:
  /** The schema registered tables belong to */
  val schemaName: String = "main"

  /** Open an engine that resolves relative paths against the current folder */
  def open(): EmbeddedEngine = open(".")

  /** Open an engine that resolves relative paths against `baseDir` */
  def open(baseDir: String): EmbeddedEngine = new EmbeddedEngine(baseDir)

  // Statement results nest in lists and plan results; walk them in statement order
  private def flatten(r: QueryResult): Seq[QueryResult] =
    r match
      case l: QueryResultList =>
        l.list.flatMap(flatten)
      case p: PlanResult =>
        flatten(p.result)
      case other =>
        Seq(other)

  private def firstError(r: QueryResult): Option[Throwable] = flatten(r)
    .flatMap(_.getError)
    .headOption
    .map {
      case e: WvletLangException =>
        e
      case e =>
        StatusCode.INTERNAL_ERROR.newException(Option(e.getMessage).getOrElse(e.toString), e)
    }

  private def lastTable(r: QueryResult): Option[TableRows] =
    flatten(r)
      .collect { case t: TableRows =>
        t
      }
      .lastOption

end EmbeddedEngine
