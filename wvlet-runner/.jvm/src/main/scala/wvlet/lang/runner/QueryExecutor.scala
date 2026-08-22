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

import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.connector.codec.JDBCCodec
import wvlet.lang.api.LinePosition
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.compiler.query.QuerySelector
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.DataType.UnresolvedType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.compiler.connector.QueryResult as XPQueryResult
import wvlet.lang.connector.DBConnector
import wvlet.lang.connector.Connector
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.connector.SourceTableStaging
import wvlet.uni.json.JSON
import wvlet.uni.log.LogLevel
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver
import wvlet.uni.weaver.codec.PrimitiveWeaver.given

import java.sql.SQLException
import scala.collection.immutable.ListMap
import scala.util.Try

case class QueryExecutorConfig(rowLimit: Int = 40)

class QueryExecutor(
    dbConnectorProvider: ConnectorProvider,
    defaultProfile: Profile,
    workEnv: WorkEnv,
    private var config: QueryExecutorConfig = QueryExecutorConfig()
) extends LogSupport
    with AutoCloseable:

  def setRowLimit(limit: Int): QueryExecutor =
    config = config.copy(rowLimit = limit)
    this

  override def close(): Unit = {
    // DB Connector will be closed by Airframe DI
  }

  def getDBConnector(profile: Profile): DBConnector = dbConnectorProvider.getConnector(profile)

  def getDBConnector(config: ConnectorConfig): DBConnector = dbConnectorProvider.getDBConnector(
    config
  )

  def getConnector(config: ConnectorConfig): Connector = dbConnectorProvider.getConnector(config)

  /** True when the config names a SQL-engine connector (as opposed to a source connector) */
  def isEngineConnector(config: ConnectorConfig): Boolean = dbConnectorProvider.isEngineType(
    config.`type`
  )

  // The engine statements execute on: the profile's default engine until a `use <connector>`
  // statement switches it (#1861 Phase 2). Like the global default catalog/schema this is
  // session-level state: concurrent sessions must use separate QueryExecutor instances, as the
  // server does by building one script runner per client session (ScriptRunnerSessions, #1867)
  private var activeEngine: ConnectorConfig = defaultProfile.defaultEngine

  private def activeDBConnector: DBConnector = dbConnectorProvider.getDBConnector(activeEngine)

  // Resolve a profile connector name to a live connector, for per-stage `on <connector>` in
  // flows and cross-connector staging
  private def profileEngineResolver(name: String): Connector = defaultProfile
    .connectors
    .find(_.name == name)
    .map(dbConnectorProvider.getConnector)
    .getOrElse(
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Connector '${name}' is not defined in profile '${defaultProfile.name}' " +
            s"(available: ${defaultProfile.connectors.map(_.name).mkString(", ")})"
        )
    )

  /**
    * Switch the active connector: `use <connector>[.<catalog>].<schema>`. Subsequent statements
    * plan and execute on the selected connector, and its catalog drives name resolution and the SQL
    * dialect
    */
  private def switchConnector(parts: List[String])(using context: Context): QueryResult =
    val connectorName = parts.head
    val config        = defaultProfile
      .connectors
      .find(_.name == connectorName)
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Connector '${connectorName}' is not defined in profile '${defaultProfile.name}' " +
              s"(available: ${defaultProfile.connectors.map(_.name).mkString(", ")})"
          )
      )
    val (catalogName, schemaName) =
      parts.tail match
        case Nil =>
          (config.catalog, config.schema)
        case schema :: Nil =>
          (config.catalog, Some(schema))
        case catalog :: schema :: Nil =>
          (Some(catalog), Some(schema))
        case _ =>
          throw StatusCode
            .SYNTAX_ERROR
            .newException(
              s"Invalid connector reference: ${parts.mkString(".")}. " +
                "Expected format: <connector>, <connector>.<schema>, or <connector>.<catalog>.<schema>"
            )
    val schema  = schemaName.getOrElse("main")
    val catalog = catalogName.getOrElse(
      // Without a catalog the switched engine could not drive name resolution or dialect
      // selection, silently leaving the previous engine's catalog active
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Connector '${connectorName}' has no catalog configured. Add \"catalog\" to its " +
            s"profile entry or switch with `use ${connectorName}.<catalog>.<schema>`"
        )
    )
    // Resolve the connector and its catalog BEFORE committing any state, so a connection
    // failure leaves the previous engine fully active
    val connector  = dbConnectorProvider.getDBConnector(config)
    val newCatalog = connector.getCatalog(catalog, schema)
    activeEngine = config
    context.global.defaultCatalog = newCatalog
    context.global.defaultSchema = schema
    workEnv.info(s"Switched to connector: ${connectorName}")
    QueryResult.empty

  end switchConnector

  // Every profile connector becomes an activation target under its instance name, delivering
  // stage outputs through its MCP-shaped tools (e.g. activate('slack', tool: 'post_message'))
  private def profileActivationSinks: List[ActivationSink] =
    FlowExecutor.defaultActivationSinks ++
      defaultProfile
        .connectors
        .map(c => ConnectorActivationSink(c.name, () => dbConnectorProvider.getConnector(c)))

  /**
    * Materialize tables of source connectors (non-engine connectors like Slack) referenced by the
    * plan into session tables on the active engine, and point the plan at them
    */
  private def stageSourceTables(plan: LogicalPlan): (LogicalPlan, List[String]) =
    val staged    = scala.collection.mutable.Map.empty[(String, String), String]
    val rewritten = plan.transformUp {
      case t @ TableScan(_, _, _, _, Some(sourceName)) if sourceName != activeEngine.name =>
        val stagingTable = staged.getOrElseUpdate(
          (sourceName, t.name.name), {
            val source = profileEngineResolver(sourceName)
            val rows   = SourceTableStaging.readTableAsJsonRows(source, List(t.name.name))
            // ULID-suffixed so concurrent queries staging the same source never collide;
            // the table is dropped when this query finishes
            val staging =
              s"__wv_src_${sourceName}_${t.name.name}_${wvlet
                  .uni
                  .util
                  .ULID
                  .newULIDString
                  .toLowerCase}"
            workEnv.info(s"Staging ${sourceName}.${t.name.name} (${rows.size} rows) as ${staging}")
            SourceTableStaging.loadJsonRows(
              activeDBConnector,
              activeEngine.name,
              staging,
              rows,
              t.schema.fields
            )
            staging
          }
        )
        AliasedRelation(
          TableRef(DoubleQuotedIdentifier(stagingTable, t.span), t.span),
          UnquotedIdentifier(t.name.name, t.span),
          None,
          t.span
        )
    }
    (rewritten, staged.values.toList)

  end stageSourceTables

  def executeSingleSpec(sourceFolder: String, file: String): QueryResult =
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(sourceFolder), workEnv = WorkEnv(sourceFolder))
    )
    compiler.compilationUnitsInSourcePaths.find(_.sourceFile.fileName == file) match
      case Some(unit) =>
        val result = compiler.compileSingleUnit(unit)
        executeSingle(unit, result.context)
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"File not found: ${file}")

  def executeSelectedStatement(
      u: CompilationUnit,
      querySelection: QuerySelection,
      linePosition: LinePosition,
      rootContext: Context
  ): QueryResult =
    workEnv.info(s"Executing ${u.sourceFile.fileName} (${linePosition}, ${querySelection})")
    val targetStatement: LogicalPlan = QuerySelector.selectQuery(u, linePosition, querySelection)
    trace(s"Selected statement: ${targetStatement}, ${querySelection}")
    val ctx = rootContext.withCompilationUnit(u).newContext(Symbol.NoSymbol)

    val executionPlan = ExecutionPlanner.plan(u, targetStatement)(using ctx)
    val result        = execute(executionPlan, ctx)

    workEnv.info(s"Completed ${u.sourceFile.fileName}")
    result

  end executeSelectedStatement

  def executeSingle(u: CompilationUnit, rootContext: Context): QueryResult =
    workEnv.info(s"Executing ${u.sourceFile.fileName}")
    val ctx = rootContext.withCompilationUnit(u).newContext(Symbol.NoSymbol)

    // val executionPlan = u.executionPlan // ExecutionPlanner.plan(u, ctx)
    val executionPlan = ExecutionPlanner.plan(u, ctx)
    val result        = execute(executionPlan, ctx)
    workEnv.info(s"Completed ${u.sourceFile.fileName}")
    result

  def execute(executionPlan: ExecutionPlan, context: Context): QueryResult =
    var lastResult: QueryResult = QueryResult.empty
    val results                 = List.newBuilder[QueryResult]

    // TODO: Use an external reporting object to collect the results
    def report(r: QueryResult): QueryResult =
      if !r.isEmpty then
        results += r
        // Update the last result only when there is no error
        if r.isSuccessfulQueryResult then
          // TODO Add a unique name to the last result
          trace(s"last result is updated:\n${r}")
          lastResult = r
        // log results
        def isMultiline(s: String): Boolean = s.split("\n").size > 1

        r match
          case t: TestSuccess =>
            if isMultiline(t.msg) then
              workEnv.debug(s"Test passed: (${t.loc.locationString})\n${t.msg}")
            else
              workEnv.debug(s"Test passed: ${t.msg} (${t.loc.locationString})")
          case t: TestFailure =>
            if isMultiline(t.msg) then
              workEnv.error(s"Test failed: (${t.loc.locationString})\n${t.msg}")
            else
              workEnv.error(s"Test failed: ${t.msg} (${t.loc.locationString})")
          case w: WarningResult =>
            warn(s"${w.msg} (${w.loc.locationString})")
            workEnv.warn(s"Warning: ${w.msg}")
          case _ =>

      r

    def process(e: ExecutionPlan)(using Context): QueryResult =
      e match
        case ExecuteQuery(plan) =>
          report(executeQuery(plan))
        case ExecuteSave(save, queryPlan) =>
          // Evaluate test/debug if exists
          report(process(queryPlan))
          report(executeSave(save))
        case d @ ExecuteDebug(debugPlan, debugExecutionPlan) =>
          val debugInput = lastResult
          executeDebug(d, lastResult)
          debugInput
        case ExecuteTest(test) =>
          trace(s"run test: ${test.testExpr}")
          report(executeTest(test, lastResult))
        case ExecuteTasks(tasks) =>
          val results = tasks.map { task =>
            process(task)
          }
          QueryResult.fromList(results)
        case ExecuteCommand(e) =>
          // Command produces no QueryResult other than errors
          report(executeCommand(e))
        case ExecuteFlow(flow) =>
          scala
            .util
            .Using
            .resource(FlowRunStore.forWorkEnv(workEnv)) { store =>
              val flowExecutor = FlowExecutor(
                activeDBConnector,
                workEnv,
                registry = Some(store),
                engineResolver = Some(profileEngineResolver),
                defaultEngineName = activeEngine.name,
                activationSinks = profileActivationSinks
              )
              report(flowExecutor.execute(flow))
            }
        case ExecuteValDef(v) =>
          val expr = ExpressionEvaluator.eval(v.expr)(using context)
          v.symbol.symbolInfo = ValSymbolInfo(
            context.owner,
            v.symbol,
            v.name,
            expr.dataType,
            expr,
            context.compilationUnit
          )
          context.enter(v.symbol)
          QueryResult.empty
        case ExecuteNothing =>
          report(QueryResult.empty)

    process(executionPlan)(using context)
    // Prefer the last successful result when no results were accumulated.
    // This guards against execution paths that update `lastResult` but do not
    // add to the `results` builder (e.g., nested task flows).
    val aggregated = QueryResult.fromList(results.result())
    if aggregated.isEmpty && !lastResult.isEmpty then
      lastResult
    else
      aggregated

  end execute

  private def executeStatement(sqls: List[String])(using context: Context): Unit = sqls.foreach:
    sql =>
      workEnv.info(s"Executing SQL:\n${sql}")
      debug(s"Executing SQL:\n${sql}")
      given monitor: QueryProgressMonitor = context.queryProgressMonitor
      try
        activeDBConnector.execute(sql)
      catch
        case e: SQLException =>
          throw StatusCode.SYNTAX_ERROR.newException(s"${e.getMessage}\n[sql]\n${sql}", e)

  private def executeCommand(cmd: Command)(using context: Context): QueryResult =
    given monitor: QueryProgressMonitor = context.queryProgressMonitor
    cmd match
      case e: ExecuteExpr =>
        val cmd = GenSQL.generateExecute(e.expr)
        executeStatement(List(cmd))
        QueryResult.empty
      case e: ExplainPlan =>
        // Expand RawSQL to a logical plan
        val plan = e
          .child
          .transformUp { case r: RawSQL =>
            val sql     = SqlGenerator(CodeFormatterConfig(sqlDBType = context.dbType)).print(r.sql)
            val unit    = CompilationUnit.fromSqlString(sql)
            val sqlPlan = SqlParser(unit).parse()
            var query: Option[Query] = None
            sqlPlan.traverseOnce { case q: Query =>
              query = Some(q)
            }
            query.getOrElse {
              throw StatusCode.SYNTAX_ERROR.newException(s"Failed to find query within SQL: ${sql}")
            }
          }
        val logicalPlanString = plan.pp
        println(s"\n${logicalPlanString}")
        QueryResult.empty
      case s: ShowQuery =>
        context.findTermSymbolByName(s.name.fullName) match
          case Some(sym) =>
            sym.tree match
              case md: ModelDef =>
                sym.symbolInfo match
                  case m: ModelSymbolInfo =>
                    val query = m
                      .compilationUnit
                      .text(md.child.span)
                      // Remove indentation
                      .split("\n")
                      .map(_.trim)
                      .mkString("\n")

                    // TODO Report query in the provided output
                    println(query)
                  case _ =>
              // TODO Support SelectAsAlias, already resolved models, etc.
              case _ =>
            QueryResult.empty
          case None =>
            WarningResult(s"${s.name} is not found", s.sourceLocation(using context))
      case u: UseConnector =>
        switchConnector(u.connector.fullName.split("\\.").toList)(using context)
      case u: UseSchema =>
        // Update the global context with the new schema/catalog. Connector names of the active
        // profile shadow schema names, so `use td` switches the connector when `td` is one.
        val schemaName = u.schema
        val fullName   = schemaName.fullName
        val parts      = fullName.split("\\.").toList
        parts match
          case name :: rest if defaultProfile.connectors.exists(_.name == name) =>
            switchConnector(parts)(using context)
          case schema :: Nil =>
            // use schema <schema_name>
            context.global.defaultSchema = schema
            workEnv.info(s"Switched to schema: ${schema}")
            QueryResult.empty
          case catalogName :: schema :: Nil =>
            // use schema <catalog_name>.<schema_name>
            // For now, we only update the schema since catalog switching requires more complex handling
            context.global.defaultSchema = schema
            workEnv.info(s"Switched to schema: ${schema}")
            QueryResult.empty
          case _ =>
            throw StatusCode
              .SYNTAX_ERROR
              .newException(
                s"Invalid schema name: ${fullName}. Expected format: <schema_name> or <catalog_name>.<schema_name>"
              )
      case d: DescribeInput =>
        // For now, just return empty result since DESCRIBE INPUT is mainly for parsing validation
        // In a full implementation, this would query the prepared statement input metadata
        workEnv.info(s"DESCRIBE INPUT ${d.name.fullName}")
        QueryResult.empty
      case d: DescribeOutput =>
        // For now, just return empty result since DESCRIBE OUTPUT is mainly for parsing validation
        // In a full implementation, this would query the prepared statement output metadata
        workEnv.info(s"DESCRIBE OUTPUT ${d.name.fullName}")
        QueryResult.empty
    end match

  end executeCommand

  private def executeSave(save: Save)(using context: Context): QueryResult =
    trace(s"Executing save:\n${save.pp}")
    workEnv.trace(s"Executing save: ${save.pp}")
    save match
      case s: SaveTo if s.isForFile && !context.dbType.supportSaveAsFile =>
        // Cross-engine handoff (e.g., Trino -> DuckDB) for local file output
        executeSaveToLocalFileViaDuckDB(s)
      case _ =>
        val statements = GenSQL.generateSaveSQL(save, context)
        executeStatement(statements)
        QueryResult.empty

  /**
    * Cross-engine SaveTo implementation for local files when the current DB doesn't support direct
    * file output. Streams from the current connector (e.g., Trino) and writes via DuckDB. Temporary
    * artifacts live under workEnv.cacheFolder.
    *
    * Note: This is a scaffolding entry point; full ingestion will follow.
    */
  private def executeSaveToLocalFileViaDuckDB(save: SaveTo)(using context: Context): QueryResult =
    import java.io.{BufferedWriter, File, FileOutputStream, FileWriter, OutputStreamWriter}
    import java.sql.ResultSet
    import java.util.zip.GZIPOutputStream
    import wvlet.uni.util.ULID
    import java.nio.file.{Files, Paths}
    import scala.util.Using

    def isRemotePath(p: String): Boolean = p.startsWith("s3://") || p.startsWith("https://")

    val targetPath = context.dataFilePath(save.targetName)
    if isRemotePath(targetPath) then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Remote path is not supported for local file save via DuckDB handoff: ${targetPath}"
        )

    if !targetPath.toLowerCase.endsWith(".parquet") then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(s"Only Parquet is supported for Trino local save. Given: ${targetPath}")

    val uid       = ULID.newULIDString
    val stageDir  = File(s"${workEnv.cacheFolder}/wvlet/stage/${uid}")
    val jsonlFile = File(stageDir, "export.jsonl.gz")
    if !stageDir.exists() then
      Files.createDirectories(Paths.get(stageDir.getPath))

    def writeJSONL(rs: ResultSet, out: File): Long =
      var rowCount = 0L
      val rowCodec = summon[Weaver[ListMap[String, Any]]]
      Using.resource(BufferedWriter(OutputStreamWriter(GZIPOutputStream(FileOutputStream(out))))) {
        w =>
          val codec = JDBCCodec(rs)
          val it    = codec.mapMsgPackMapRows { msgpack =>
            rowCodec.unweave(msgpack)
          }
          while it.hasNext do
            val row = it.next()
            w.write(rowCodec.toJson(row))
            w.newLine()
            rowCount += 1
      }
      rowCount

    // Cross-platform variant for backends that don't expose a JDBC ResultSet (Trino HTTP).
    // Stream the materialized QueryResult through the same JSON writer the JDBC path produces.
    def writeJSONLFromXP(r: XPQueryResult, out: File): Long =
      val rowCodec = summon[Weaver[ListMap[String, Any]]]
      val names    = r.columns.map(_.name.name)
      Using.resource(BufferedWriter(OutputStreamWriter(GZIPOutputStream(FileOutputStream(out))))) {
        w =>
          r.rows
            .foreach { row =>
              val pairs = names
                .iterator
                .zip(row.values.iterator)
                .map { case (n, v) =>
                  n -> v.orNull.asInstanceOf[Any]
                }
              w.write(rowCodec.toJson(ListMap.from(pairs)))
              w.newLine()
            }
      }
      r.rowCount.toLong

    // 1) Execute on Trino (or current engine) and dump to JSONL
    val baseSQL = GenSQL.generateSQLFromRelation(save.child, addHeader = false)
    given monitor: QueryProgressMonitor = context.queryProgressMonitor
    val sourceConn                      = activeDBConnector
    val n                               =
      sourceConn.sqlConnector match
        case Some(sc) =>
          // HTTP-based engines (e.g. Trino): no JDBC ResultSet, materialize via SqlConnector.
          writeJSONLFromXP(sc.execute(baseSQL.sql), jsonlFile)
        case None =>
          sourceConn.runQuery(baseSQL.sql) { rs =>
            writeJSONL(rs, jsonlFile)
          }
    workEnv.info(s"Exported ${n} rows to compressed JSONL at ${jsonlFile.getPath}")

    // 2) Directly COPY from read_json_auto() to Parquet via DuckDB
    val duck                  = dbConnectorProvider.getConnector(DBType.DuckDB, None)
    def sq(s: String): String = s.replace("'", "''")
    import scala.util.control.NonFatal
    try
      val copyOpts = "(FORMAT 'parquet', USE_TMP_FILE true)"
      val copySQL  =
        s"copy (select * from read_json_auto('${sq(jsonlFile.getPath)}')) to '${sq(
            targetPath
          )}' ${copyOpts}"
      duck.execute(copySQL)
    finally
      def safe(msg: String)(f: => Unit): Unit =
        try
          f
        catch
          case NonFatal(e) =>
            workEnv.warn(s"${msg}: ${e.getMessage}")
      safe(s"Failed to delete temporary compressed JSONL ${jsonlFile.getPath}") {
        jsonlFile.delete()
      }
      safe(s"Failed to delete stage dir ${stageDir.getPath}") {
        stageDir.delete()
      }

    workEnv.info(s"Saved result to ${targetPath}")
    QueryResult.empty

  end executeSaveToLocalFileViaDuckDB

  /**
    * Resolve a `run flow <name>` reference to its flow definition via the symbol table
    */
  private def resolveFlow(rf: RunFlow)(using context: Context): FlowDef =
    context.findTermSymbolByName(rf.flowName.fullName).map(_.tree) match
      case Some(f: FlowDef) =>
        f
      case _ =>
        throw StatusCode
          .FLOW_NOT_FOUND
          .newException(
            s"Flow '${rf.flowName.fullName}' is not found",
            rf.sourceLocation(using context)
          )

  /**
    * Execute flow runs embedded in a query plan and replace each RunFlow node with a reference to a
    * materialized flow-run summary table (stage, state, attempts, error), so that downstream query
    * operators and test statements work on the summary rows via regular SQL
    */
  private def runEmbeddedFlows(q: Relation)(using context: Context): Relation =
    def sqlLit(s: String): String = s"'${s.replaceAll("'", "''")}'"

    q.transformUp { case rf: RunFlow =>
        val flow       = resolveFlow(rf)
        val connector  = activeDBConnector
        val flowResult =
          scala
            .util
            .Using
            .resource(FlowRunStore.forWorkEnv(workEnv)) { store =>
              FlowExecutor(
                connector,
                workEnv,
                registry = Some(store),
                engineResolver = Some(profileEngineResolver),
                defaultEngineName = activeEngine.name,
                activationSinks = profileActivationSinks
              ).execute(flow, args = rf.args)
            }

        given monitor: QueryProgressMonitor = context.queryProgressMonitor

        val summaryTable = s"__wv_flow_run_${flowResult.runId.toLowerCase}"
        connector.execute(
          s"""create or replace temp table "${summaryTable}" ("stage" varchar, "state" varchar, "attempts" bigint, "error" varchar)"""
        )
        if flowResult.stageResults.nonEmpty then
          val rows = flowResult
            .stageResults
            .map { s =>
              val err = s.error.map(e => sqlLit(e.getMessage)).getOrElse("null")
              s"(${sqlLit(s.name)}, ${sqlLit(s.state.stateName)}, ${s.attempts}, ${err})"
            }
            .mkString(", ")
          connector.execute(s"""insert into "${summaryTable}" values ${rows}""")
        TableRef(DoubleQuotedIdentifier(summaryTable, rf.span), rf.span)
      }
      .asInstanceOf[Relation]

  end runEmbeddedFlows

  /**
    * Execute ad-hoc connector tool calls (`call <connector>.<tool>(...)`) embedded in a query plan
    * and replace each CallTool node with a reference to a materialized single-row invocation
    * summary table (connector, tool, status, content), so that downstream query operators and test
    * statements work on the result via regular SQL
    */
  private def runEmbeddedToolCalls(q: Relation)(using context: Context): Relation =
    def sqlLit(s: String): String = s"'${s.replaceAll("'", "''")}'"

    def jsonArgValue(toolName: String, e: Expression): JSON.JSONValue =
      e match
        case s: StringLiteral =>
          JSON.JSONString(s.unquotedValue)
        case l: LongLiteral =>
          JSON.JSONLong(l.value)
        case d: DoubleLiteral =>
          JSON.JSONDouble(d.value)
        case d: DecimalLiteral =>
          JSON.JSONDouble(d.value.toDouble)
        case _: TrueLiteral =>
          JSON.JSONBoolean(true)
        case _: FalseLiteral =>
          JSON.JSONBoolean(false)
        case _: NullLiteral =>
          JSON.JSONNull()
        // Negative (or explicitly signed) numbers parse as a unary expression over the literal
        case a: ArithmeticUnaryExpr =>
          jsonArgValue(toolName, a.child) match
            case JSON.JSONLong(v) =>
              JSON.JSONLong(
                if a.sign == Sign.Negative then
                  -v
                else
                  v
              )
            case JSON.JSONDouble(v) =>
              JSON.JSONDouble(
                if a.sign == Sign.Negative then
                  -v
                else
                  v
              )
            case _ =>
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(s"Tool '${toolName}' arguments must be literal values, found: ${e}")
        case l: Literal =>
          JSON.JSONString(l.stringValue)
        case other =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Tool '${toolName}' arguments must be literal values, found: ${other}")

    q.transformUp { case ct: CallTool =>
        val connectorName = ct.connectorName.fullName
        val toolName      = ct.toolName.fullName
        val config        = defaultProfile
          .connectors
          .find(_.name == connectorName)
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Connector '${connectorName}' is not found in profile '${defaultProfile.name}'",
                ct.sourceLocation(using context)
              )
          )
        val target = dbConnectorProvider.getConnector(config)
        if !target.tools.exists(_.name == toolName) then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Connector '${connectorName}' has no tool '${toolName}' (available: ${target
                  .tools
                  .map(_.name)
                  .mkString(", ")})",
              ct.sourceLocation(using context)
            )
        val args = ct
          .args
          .map { arg =>
            val name = arg
              .name
              .getOrElse(
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(
                    s"Tool arguments must be named, e.g. ${toolName}(channel: '#general')",
                    ct.sourceLocation(using context)
                  )
              )
            name.name -> jsonArgValue(toolName, arg.value)
          }
        val result = target.invoke(toolName, JSON.JSONObject(args))
        val status =
          if result.isError then
            "error"
          else
            "ok"
        val content =
          result.content match
            case s: JSON.JSONString =>
              s.v
            case other =>
              other.toJSON

        given monitor: QueryProgressMonitor = context.queryProgressMonitor

        val summaryTable = s"__wv_call_${wvlet.uni.util.ULID.newULIDString.toLowerCase}"
        activeDBConnector.execute(
          s"""create or replace temp table "${summaryTable}" ("connector" varchar, "tool" varchar, "status" varchar, "content" varchar)"""
        )
        activeDBConnector.execute(
          s"""insert into "${summaryTable}" values (${sqlLit(connectorName)}, ${sqlLit(
              toolName
            )}, ${sqlLit(status)}, ${sqlLit(content)})"""
        )
        TableRef(DoubleQuotedIdentifier(summaryTable, ct.span), ct.span)
      }
      .asInstanceOf[Relation]

  end runEmbeddedToolCalls

  private def executeQuery(plan0: LogicalPlan)(using context: Context): QueryResult =
    // Tables resolved through a profile connector name must live on the engine active when
    // this query runs (an in-file `use <connector>` earlier in the unit has already switched
    // activeEngine by now) — except source connectors (Slack etc.), whose tables are staged
    // into the active engine below
    val foreignEngines = scala.collection.mutable.Set.empty[String]
    plan0.traverse { case t: TableScan =>
      t.connectorName
        .filter(_ != activeEngine.name)
        // Classified from the config type: instantiating the connector here would open a
        // connection just to reject the query
        .filter(name =>
          defaultProfile
            .connectors
            .find(_.name == name)
            .forall(c => dbConnectorProvider.isEngineType(c.`type`))
        )
        .foreach(foreignEngines += _)
    }
    if foreignEngines.nonEmpty then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Query references connector(s) ${foreignEngines.mkString(
              ", "
            )} but the active engine is '${activeEngine
              .name}'. Cross-connector queries are not supported yet; run `use ${foreignEngines
              .head}` first to execute on that connector"
        )
    val (plan, stagedTables) = stageSourceTables(plan0)
    try executeQueryPlan(plan)
    finally
      // Staged source tables are per-query scratch space; drop them so persistent engine
      // databases do not accumulate them
      stagedTables.foreach { staging =>
        try
          activeDBConnector.execute(s"""drop table if exists "${staging}"""")
        catch
          case scala.util.control.NonFatal(e) =>
            debug(s"Failed to drop staging table ${staging}: ${e.getMessage}")
      }
  end executeQuery

  private def executeQueryPlan(plan: LogicalPlan)(using context: Context): QueryResult =
    trace(s"Executing query: ${plan.pp}")
    workEnv.trace(s"Executing plan: ${plan.pp}")
    plan match
      case q0: Relation =>
        // Execute any embedded flow runs and ad-hoc tool calls first, replacing them with their
        // summary tables
        val q = runEmbeddedToolCalls(runEmbeddedFlows(q0))
        // Decide whether to auto-switch to DuckDB for simple local file reads
        def isRemotePath(p: String): Boolean = p.startsWith("s3://") || p.startsWith("https://")
        def prefersDuckDBForLocalRead(r: Relation): Boolean =
          var hasLocalFile    = false
          var hasNonFileInput = false
          r.traverse {
            case f: FileScan =>
              if !isRemotePath(f.filePath) then
                hasLocalFile = true
            case f: FileRef =>
              if !isRemotePath(f.filePath) then
                hasLocalFile = true
            case _: TableRef | _: TableFunctionCall | _: RawSQL | _: ModelScan =>
              hasNonFileInput = true
          }
          hasLocalFile && !hasNonFileInput

        val useDuck      = prefersDuckDBForLocalRead(q)
        val generatedSQL = GenSQL.generateSQLFromRelation(q)
        if useDuck then
          workEnv.info("Auto-switched to DuckDB for local file read.")
        workEnv.info(s"Executing SQL:\n${generatedSQL.sql}")
        debug(s"Executing SQL:\n${generatedSQL.sql}")
        try
          given monitor: QueryProgressMonitor = context.queryProgressMonitor
          val connector                       =
            if useDuck then
              dbConnectorProvider.getConnector(DBType.DuckDB, None)
            else
              activeDBConnector

          val result =
            connector.sqlConnector match
              case Some(sc) =>
                // Engines that execute over HTTP instead of JDBC (Trino) return rows as
                // `Seq[Option[String]]` per column — no JDBC ResultSet, no `JDBCCodec`. DuckDB
                // deliberately stays on the JDBC branch below even though it has a stateful
                // `asSqlConnector` view: the cross-platform rows are varchar-coerced, and the
                // JDBC path preserves typed values for `test _.rows` assertions and the web UI.
                tableRowsFromXP(sc.execute(generatedSQL.sql))
              case None =>
                connector.runQuery(generatedSQL.sql) { rs =>
                  val metadata = rs.getMetaData
                  val fields   =
                    for i <- 1 to metadata.getColumnCount
                    yield NamedType(
                      Name.termName(metadata.getColumnName(i)),
                      Try(DataType.parse(metadata.getColumnTypeName(i))).getOrElse {
                        UnresolvedType(metadata.getColumnTypeName(i))
                      }
                    )
                  val outputType = SchemaType(None, Name.NoTypeName, fields.toList)
                  trace(outputType)

                  val codec    = JDBCCodec(rs)
                  val rowCodec = summon[Weaver[ListMap[String, Any]]]
                  var rowCount = 0
                  val it       = codec.mapMsgPackMapRows { msgpack =>
                    if rowCount <= config.rowLimit then
                      rowCodec.unweave(msgpack)
                    else
                      null
                  }

                  val rows = Seq.newBuilder[ListMap[String, Any]]
                  while it.hasNext do
                    val row = it.next()
                    rowCount += 1
                    if row != null && rowCount <= config.rowLimit then
                      rows += row

                  TableRows(outputType, rows.result(), rowCount)
                }
          result
        catch
          case e: SQLException =>
            // Preserve existing error shape including the SQL text
            throw StatusCode
              .SYNTAX_ERROR
              .newException(s"${e.getMessage}\n[sql]\n${generatedSQL.sql}", e)
        end try
      case other =>
        QueryResult.empty
    end match

  end executeQueryPlan

  /**
    * Adapt a cross-platform `XPQueryResult` (from `SqlConnector.execute`) into the runner's
    * `TableRows`. Rows come back as `Seq[Option[String]]`; we zip with the declared column names to
    * build the `ListMap[String, Any]` shape downstream renderers (web UI, REPL printer) already
    * expect. `None` cells map to `null` to match the JDBC path, where `rs.getString` + `wasNull()`
    * produces the same effective representation.
    */
  private def tableRowsFromXP(r: XPQueryResult): TableRows =
    val outputType    = SchemaType(None, Name.NoTypeName, r.columns.toList)
    val columnNames   = r.columns.map(_.name.name)
    val truncatedRows =
      r.rows
        .iterator
        .take(config.rowLimit)
        .map { row =>
          val pairs = columnNames
            .iterator
            .zip(row.values.iterator)
            .map { case (name, value) =>
              name -> value.orNull.asInstanceOf[Any]
            }
          ListMap.from(pairs)
        }
        .toList
    TableRows(outputType, truncatedRows, r.rowCount)

  private def executeDebug(debugPlan: ExecuteDebug, lastResult: QueryResult)(using
      context: Context
  ): QueryResult =
    val result = execute(debugPlan.debugExecutionPlan, context)
    // TODO: Output to REPL
    workEnv.info(result)
    QueryResult.empty

  private def executeTest(test: TestRelation, lastResult: QueryResult)(using
      context: Context
  ): QueryResult =

    given unit: CompilationUnit = context.compilationUnit

    def isShortString(x: Any): Boolean =
      def fitToSingleLine(x: String): Boolean = x != null && x.length < 30 && !x.contains("\n")

      x match
        case s: String =>
          fitToSingleLine(s)
        case null =>
          true
        case x if fitToSingleLine(x.toString) =>
          true
        case _ =>
          false

    def pp(x: Any): String =
      x match
        case s: Seq[?] =>
          s"[${s.map(pp).mkString(", ")}]"
        case null =>
          "null"
        case _ =>
          x.toString

    def cmpMsg(op: String, l: Any, r: Any): String =
      (l, r) match
        case (l: Any, r: Any) if isShortString(l) && isShortString(r) =>
          s"${pp(l)} ${op} ${pp(r)}"
        case _ =>
          s"${pp(l)}\n${op}\n${pp(r)}"

    def eval(e: Expression): QueryResult =
      e match
        case ShouldExpr(TestType.ShouldBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue != rightValue then
            TestFailure(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
        case Eq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue != rightValue then
            TestFailure(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
        case ShouldExpr(TestType.ShouldNotBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue == rightValue then
            TestFailure(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
        case NotEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue == rightValue then
            TestFailure(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
        case IsNull(left, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = null
          if leftValue != rightValue then
            TestFailure(s"${pp(leftValue)} was not null", e.sourceLocation)
          else
            TestSuccess(s"${pp(leftValue)} was null", e.sourceLocation)
        case IsNotNull(left, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = null
          if leftValue == rightValue then
            TestFailure(s"${pp(leftValue)} was null", e.sourceLocation)
          else
            TestSuccess(s"${pp(leftValue)} was not null", e.sourceLocation)
        case ShouldExpr(TestType.ShouldContain, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          (leftValue, rightValue) match
            case (l: String, r: String) =>
              if l.contains(r) then
                TestSuccess(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestFailure(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case (l: List[?], r: Any) =>
              if l.contains(r) then
                TestSuccess(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestFailure(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case _ =>
              WarningResult(
                s"`contain` operator is not supported for: ${leftValue} and ${rightValue}",
                e.sourceLocation
              )
        case ShouldExpr(TestType.ShouldNotContain, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          (leftValue, rightValue) match
            case (l: String, r: String) =>
              if l.contains(r) then
                TestFailure(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestSuccess(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case (l: List[?], r: Any) =>
              if l.contains(r) then
                TestFailure(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestSuccess(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case _ =>
              WarningResult(
                s"`contain` operator is not supported for: ${leftValue} and ${rightValue}",
                e.sourceLocation
              )
        case LessThanOrEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x <= 0 =>
                TestSuccess(
                  cmpMsg("was less than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
              case _ =>
                TestFailure(
                  cmpMsg("was not less than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case LessThan(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x < 0 =>
                TestSuccess(cmpMsg("was less than", leftValue, rightValue), e.sourceLocation)
              case _ =>
                TestFailure(cmpMsg("was not less than", leftValue, rightValue), e.sourceLocation)
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case GreaterThanOrEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x >= 0 =>
                TestSuccess(
                  cmpMsg("was greater than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
              case _ =>
                TestFailure(
                  cmpMsg("was not greater than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case GreaterThan(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x > 0 =>
                TestSuccess(cmpMsg("was greater than", leftValue, rightValue), e.sourceLocation)
              case _ =>
                TestFailure(cmpMsg("was not greater than", leftValue, rightValue), e.sourceLocation)
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case _ =>
          WarningResult(s"Unsupported test expression: ${e}", e.sourceLocation)

    def cmpAny(l: Any, r: Any): Option[Int] =
      (l, r) match
        case (l: String, r: String) =>
          Some(l.compareTo(r))
        case (l: Int, r: Int) =>
          Some(l.compareTo(r))
        case (l: Int, r: Long) =>
          Some(l.toLong.compareTo(r))
        case (l: Long, r: Long) =>
          Some(l.compareTo(r))
        case (l: Long, r: Int) =>
          Some(l.compareTo(r.toLong))
        case (l: Float, r: Float) =>
          Some(l.compareTo(r))
        case (l: Double, r: Double) =>
          Some(l.compareTo(r))
        case (l: Boolean, r: Boolean) =>
          Some(l.compareTo(r))
        case (l: BigDecimal, r: BigDecimal) =>
          Some(l.compareTo(r))
        case _ =>
          None

    def evalOp(e: Expression): Any =
      e match
        case DotRef(i: Identifier, name, _, _) if i.fullName == "_" =>
          name.leafName match
            case "output" =>
              lastResult.toPrettyBox()
            case "columns" =>
              lastResult match
                case t: TableRows =>
                  t.schema.fields.map(_.name.name).toList
                case _ =>
                  List.empty
            case "size" =>
              lastResult match
                case t: TableRows =>
                  t.totalRows
                case _ =>
                  0
            case "json" =>
              lastResult match
                case t: TableRows =>
                  t.toJsonLines
                case _ =>
                  ""
            case "rows" =>
              lastResult match
                case t: TableRows =>
                  t.rows.map(_.values.toList).toList
                case _ =>
                  List.empty
            case other =>
              throw StatusCode
                .TEST_FAILED
                .newException(s"Unsupported result inspection function: _.${other}")
          end match
        case l: StringLiteral =>
          l.unquotedValue
        case l: LongLiteral =>
          l.value
        case d: DoubleLiteral =>
          d.value
        case b: BooleanLiteral =>
          b.booleanValue
        case d: DecimalLiteral =>
          d.value
        case n: NullLiteral =>
          null
        case a: ArrayConstructor =>
          a.values.map(evalOp)
        case m: MapValue =>
          m.entries
            .map { x =>
              evalOp(x.key) -> evalOp(x.value)
            }
            .toMap
        case other =>
          workEnv.warn(s"Test expression ${e} is not supported yet.")
          ()

    def trim(v: Any): Any =
      v match
        case s: String =>
          s.trim
        case _ =>
          v

    try
      eval(test.testExpr)
    catch
      case e: WvletLangException =>
        TestFailure(e.getMessage, test.sourceLocation)

  end executeTest

end QueryExecutor
