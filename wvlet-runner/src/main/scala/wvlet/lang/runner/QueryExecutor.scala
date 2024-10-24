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
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.GenSQL.Indented
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.{NamedType, SchemaType, UnresolvedType}
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.log.{LogLevel, LogSupport}

import java.io.File
import java.sql.SQLException
import scala.collection.immutable.ListMap
import scala.util.Try

case class QueryExecutorConfig(rowLimit: Int = 40)

class QueryExecutor(
    dbConnector: DBConnector,
    workEnv: WorkEnv,
    private var config: QueryExecutorConfig = QueryExecutorConfig()
) extends LogSupport
    with AutoCloseable:

  def setRowLimit(limit: Int): QueryExecutor =
    config = config.copy(rowLimit = limit)
    this

  def getDBConnector: DBConnector = dbConnector

  override def close(): Unit = dbConnector.close()

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

  def executeSingle(u: CompilationUnit, rootContext: Context): QueryResult =
    workEnv.outLogger.info(s"Executing ${u.sourceFile.fileName}")
    val ctx = rootContext.withCompilationUnit(u).newContext(Symbol.NoSymbol)

    val executionPlan = ExecutionPlanner.plan(u, ctx)
    val result        = execute(executionPlan, ctx)
    workEnv.outLogger.info(s"Completed ${u.sourceFile.fileName}")
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
          debug(s"last result is updated:\n${r}")
          lastResult = r
        // log results
        r match
          case t: TestSuccess =>
            workEnv.outLogger.debug(s"Test passed: ${t.msg} (${t.loc.locationString})")
          case t: TestFailure =>
            workEnv.outLogger.error(s"Test failed: ${t.msg} (${t.loc.locationString})")
          case w: WarningResult =>
            warn(s"${w.msg} (${w.loc.locationString})")
            workEnv.outLogger.warn(s"Warning: ${w.msg}")
          case _ =>

      r

    def process(e: ExecutionPlan): QueryResult =
      e match
        case ExecuteQuery(plan) =>
          report(executeQuery(plan, context))
        case ExecuteSave(save, queryPlan) =>
          // Evaluate test/debug if exists
          report(process(queryPlan))
          report(executeSave(save)(using context))
        case ExecuteDelete(delete, queryPlan) =>
          // Evaluate test/debug if exists
          report(process(queryPlan))
          report(executeDelete(delete)(using context))
        case d @ ExecuteDebug(debugPlan, debugExecutionPlan) =>
          val debugInput = lastResult
          executeDebug(d, lastResult)(using context)
          debugInput
        case ExecuteTest(test) =>
          debug(s"run test: ${test.testExpr}")
          report(executeTest(test, lastResult)(using context))
        case ExecuteTasks(tasks) =>
          val results = tasks.map { task =>
            process(task)
          }
          QueryResult.fromList(results)
        case ExecuteCommand(e) =>
          // Command produces no QueryResult other than errors
          report(executeCommand(e, context))
        case ExecuteValDef(v) =>
          val expr = ExpressionEvaluator.eval(v.expr, context)
          v.symbol.symbolInfo = BoundedSymbolInfo(v.symbol, v.name, expr.dataType, expr)
          context.enter(v.symbol)
          QueryResult.empty
        case ExecuteNothing =>
          report(QueryResult.empty)

    process(executionPlan)
    QueryResult.fromList(results.result())

  end execute

  private def executeStatement(sqls: List[String]): Unit = dbConnector.withConnection { conn =>
    withResource(conn.createStatement()) { stmt =>
      sqls.foreach { sql =>
        workEnv.outLogger.info(s"Executing SQL:\n${sql}")
        debug(s"Executing SQL:\n${sql}")
        try
          stmt.execute(sql)
          dbConnector.processWarning(stmt.getWarnings)
        catch
          case e: SQLException =>
            throw StatusCode.SYNTAX_ERROR.newException(s"${e.getMessage}\n[sql]\n${sql}", e)
      }
    }
  }

  private def executeCommand(cmd: Command, context: Context): QueryResult =
    cmd match
      case e: ExecuteExpr =>
        val gen = GenSQL(context)
        val cmd = gen.printExpression(e.expr)(using Indented(0))
        executeStatement(List(cmd))
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
                      .split("\n").map(_.trim).mkString("\n")

                    // TODO Report query in the provided output
                    println(query)
                  case _ =>
              // TODO Support SelectAsAlias, already resolved models, etc.
              case _ =>
            QueryResult.empty
          case None =>
            WarningResult(s"${s.name} is not found", s.sourceLocation(using context))

  private def executeDelete(ops: DeleteOps)(using context: Context): QueryResult =
    val gen = GenSQL(context)
    ops match
      case d: Delete =>
        def filterExpr(x: Relation): Option[String] =
          x match
            case q: Query =>
              filterExpr(q.child)
            case f: Filter =>
              Some(gen.printExpression(f.filterExpr)(using Indented(0)))
            case l: LeafPlan =>
              None
            case other =>
              throw StatusCode
                .SYNTAX_ERROR
                .newException(s"Unsupported delete input: ${other.modelName}", other.sourceLocation)

        val filterSQL = filterExpr(d.inputRelation)
        var sql       = s"delete from ${d.targetTable.fullName}"
        filterSQL.foreach { expr =>
          sql += s"\nwhere ${expr}"
        }
        executeStatement(List(sql))
        QueryResult.empty
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"${other.modelName} is not implemented yet", other.sourceLocation)

  private def executeSave(save: Save)(using context: Context): QueryResult =
    save match
      case s: SaveAs =>
        val baseSQL           = GenSQL.generateSQL(save.inputRelation, context)
        val statements        = List.newBuilder[String]
        var needsTableCleanup = false
        val ctasCmd =
          if context.dbType.supportCreateOrReplace then
            s"create or replace table"
          else
            needsTableCleanup = true
            "create table"
        val ctasSQL = s"${ctasCmd} ${s.target.fullName} as\n${baseSQL.sql}"

        if needsTableCleanup then
          val dropSQL = s"drop table if exists ${s.target.fullName}"
          // TODO: May need to wrap drop-ctas in a transaction
          statements += dropSQL

        statements += ctasSQL
        executeStatement(statements.result())
        QueryResult.empty
      case s: SaveAsFile if context.dbType == DBType.DuckDB =>
        val baseSQL    = GenSQL.generateSQL(save.inputRelation, context)
        val targetPath = context.dataFilePath(s.path)
        val sql        = s"copy (${baseSQL.sql}) to '${targetPath}'"
        executeStatement(List(sql))
        QueryResult.empty
      case a: AppendTo =>
        val baseSQL       = GenSQL.generateSQL(save.inputRelation, context)
        val tbl           = TableName.parse(a.targetName)
        val schema        = tbl.schema.getOrElse(context.defaultSchema)
        val fullTableName = s"${schema}.${tbl.name}"
        val insertSQL =
          context.catalog.getTable(TableName.parse(fullTableName)) match
            case Some(t) =>
              s"insert into ${fullTableName}\n${baseSQL.sql}"
            case None =>
              s"create table ${fullTableName} as\n${baseSQL.sql}"

        executeStatement(List(insertSQL))
        QueryResult.empty
      case a: AppendToFile if context.dbType == DBType.DuckDB =>
        val baseSQL    = GenSQL.generateSQL(save.inputRelation, context)
        val targetPath = context.dataFilePath(a.path)
        if new File(targetPath).exists then
          val sql =
            s"""copy (
               |  (select * from '${targetPath}')
               |  union all
               |  ${baseSQL.sql}
               |)
               |to '${targetPath}' (USE_TMP_FILE true)""".stripMargin
          executeStatement(List(sql))
        else
          val sql = s"create (${baseSQL.sql}) to '${targetPath}'"
          executeStatement(List(sql))
        QueryResult.empty
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"${other.modelName} is not implemented yet for ${context.dbType}",
            other.sourceLocation
          )
    end match
    QueryResult.empty

  end executeSave

  private def executeQuery(plan: LogicalPlan, context: Context): QueryResult =
    trace(s"Executing query: ${plan.pp}")
    workEnv.outLogger.trace(s"Executing plan: ${plan.pp}")
    plan match
      case q: Relation =>
        val generatedSQL = GenSQL.generateSQL(q, context)
        workEnv.outLogger.info(s"Executing SQL:\n${generatedSQL.sql}")
        debug(s"Executing SQL:\n${generatedSQL.sql}")
        try
          val result = dbConnector.withConnection { conn =>
            withResource(conn.createStatement()) { stmt =>
              withResource(stmt.executeQuery(generatedSQL.sql)) { rs =>
                dbConnector.processWarning(stmt.getWarnings())

                val metadata = rs.getMetaData
                val fields =
                  for i <- 1 to metadata.getColumnCount
                  yield NamedType(
                    Name.termName(metadata.getColumnName(i)),
                    Try(DataType.parse(metadata.getColumnTypeName(i))).getOrElse {
                      UnresolvedType(metadata.getColumnTypeName(i))
                    }
                  )
                val outputType = SchemaType(None, Name.NoTypeName, fields)
                trace(outputType)

                val codec    = JDBCCodec(rs)
                val rowCodec = MessageCodec.of[ListMap[String, Any]]
                var rowCount = 0
                val it = codec.mapMsgPackMapRows { msgpack =>
                  if rowCount < config.rowLimit then
                    rowCodec.fromMsgPack(msgpack)
                  else
                    null
                }

                val rows = Seq.newBuilder[ListMap[String, Any]]
                while it.hasNext do
                  val row = it.next()
                  if row != null && rowCount < config.rowLimit then
                    rows += row
                  rowCount += 1

                TableRows(outputType, rows.result(), rowCount)
              }
            }
          }

          result
        catch
          case e: SQLException =>
            // TODO: Switch error code based on the error type
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
    workEnv.outLogger.info(result)
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
        case ShouldExpr(TestType.ShouldNotBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue == rightValue then
            TestFailure(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
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
        case _ =>
          WarningResult(s"Unsupported test expression: ${e}", e.sourceLocation)

    def evalOp(e: Expression): Any =
      e match
        case DotRef(c: ContextInputRef, name, _, _) =>
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
        case l: StringLiteral =>
          l.value
        case l: LongLiteral =>
          l.value
        case d: DoubleLiteral =>
          d.value
        case b: BooleanLiteral =>
          b.booleanValue
        case n: NullLiteral =>
          null
        case a: ArrayConstructor =>
          a.values.map(evalOp).toList
        case _ =>
          warn(s"Test expression ${e} is not supported yet.")
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
