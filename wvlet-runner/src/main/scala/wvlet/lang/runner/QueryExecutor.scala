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

import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.api.{LinePosition, StatusCode, WvletLangException}
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.{CodeFormatterConfig, GenSQL, SqlGenerator}
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.query.{QueryProgressMonitor, QuerySelector}
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.{NamedType, SchemaType, UnresolvedType}
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.log.{LogLevel, LogSupport}

import java.sql.SQLException
import scala.collection.immutable.ListMap
import scala.util.Try

case class QueryExecutorConfig(rowLimit: Int = 40)

class QueryExecutor(
    dbConnectorProvider: DBConnectorProvider,
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
        case ExecuteValDef(v) =>
          val expr = ExpressionEvaluator.eval(v.expr)(using context)
          v.symbol.symbolInfo = ValSymbolInfo(context.owner, v.symbol, v.name, expr.dataType, expr)
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
        getDBConnector(defaultProfile).execute(sql)
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
      case u: UseSchema =>
        // Update the global context with the new schema/catalog
        val schemaName = u.schema
        val fullName   = schemaName.fullName
        val parts      = fullName.split("\\.")
        parts.length match
          case 1 =>
            // use schema <schema_name>
            context.global.defaultSchema = fullName
            workEnv.info(s"Switched to schema: ${fullName}")
            QueryResult.empty
          case 2 =>
            // use schema <catalog_name>.<schema_name>
            val catalogName = parts(0)
            val schema      = parts(1)
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
        // In a full implementation, this would query the prepared statement metadata
        workEnv.info(s"DESCRIBE INPUT ${d.name.fullName}")
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
    import wvlet.airframe.ulid.ULID
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
      val rowCodec = MessageCodec.of[ListMap[String, Any]]
      Using.resource(BufferedWriter(OutputStreamWriter(GZIPOutputStream(FileOutputStream(out))))) {
        w =>
          val codec = JDBCCodec(rs)
          val it = codec.mapMsgPackMapRows { msgpack =>
            rowCodec.fromMsgPack(msgpack)
          }
          while it.hasNext do
            val row = it.next()
            w.write(rowCodec.toJson(row))
            w.newLine()
            rowCount += 1
      }
      rowCount

    // 1) Execute on Trino (or current engine) and dump to JSONL
    val baseSQL = GenSQL.generateSQLFromRelation(save.child, addHeader = false)
    given monitor: QueryProgressMonitor = context.queryProgressMonitor
    getDBConnector(defaultProfile).runQuery(baseSQL.sql) { rs =>
      val n = writeJSONL(rs, jsonlFile)
      workEnv.info(s"Exported ${n} rows to compressed JSONL at ${jsonlFile.getPath}")
      n
    }

    // 2) Directly COPY from read_json_auto() to Parquet via DuckDB
    val duck                  = dbConnectorProvider.getConnector(DBType.DuckDB, None)
    def sq(s: String): String = s.replace("'", "''")
    import scala.util.control.NonFatal
    try
      val copyOpts = "(FORMAT 'parquet', USE_TMP_FILE true)"
      val copySQL =
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

  private def executeQuery(plan: LogicalPlan)(using context: Context): QueryResult =
    trace(s"Executing query: ${plan.pp}")
    workEnv.trace(s"Executing plan: ${plan.pp}")
    plan match
      case q: Relation =>
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
          val connector =
            if useDuck then
              dbConnectorProvider.getConnector(DBType.DuckDB, None)
            else
              getDBConnector(defaultProfile)

          val result =
            connector.runQuery(generatedSQL.sql) { rs =>
              val metadata = rs.getMetaData
              val fields =
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
              val rowCodec = MessageCodec.of[ListMap[String, Any]]
              var rowCount = 0
              val it = codec.mapMsgPackMapRows { msgpack =>
                if rowCount <= config.rowLimit then
                  rowCodec.fromMsgPack(msgpack)
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

  end executeQuery

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
