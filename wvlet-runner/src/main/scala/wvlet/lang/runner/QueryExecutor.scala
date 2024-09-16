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

import wvlet.lang.{StatusCode, WvletLangException}
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.{NamedType, SchemaType, UnresolvedType}
import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource
import wvlet.lang.compiler.codegen.GenSQL.Indented
import wvlet.lang.model.expr.Expression
import wvlet.log.{LogLevel, LogSupport}

import java.sql.SQLException
import scala.collection.immutable.ListMap
import scala.util.{Try, Using}

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

  def executeSingle(u: CompilationUnit, context: Context): QueryResult =
    workEnv.outLogger.info(s"Executing ${u.sourceFile.fileName}")

    val executionPlan = ExecutionPlanner.plan(u, context)
    val result        = execute(executionPlan, context)
    result

  def execute(executionPlan: ExecutionPlan, context: Context): QueryResult =
    var lastResult: QueryResult = QueryResult.empty
    val results                 = List.newBuilder[QueryResult]

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
          report(executeCommand(e.expr, context))
        case ExecuteNothing =>
          report(QueryResult.empty)

    process(executionPlan)
    QueryResult.fromList(results.result())

  end execute

  private def executeCommand(e: Expression, context: Context): QueryResult =
    val gen = GenSQL(context)
    val cmd = gen.printExpression(e)(using Indented(0))
    workEnv.outLogger.info(s"Executing command:\n${cmd}")
    debug(s"Executing command:\n${cmd}")
    try
      dbConnector.withConnection { conn =>
        withResource(conn.createStatement()) { stmt =>
          stmt.execute(cmd)
          dbConnector.processWarning(stmt.getWarnings())
          QueryResult.empty
        }
      }
    catch
      case e: SQLException =>
        throw StatusCode.SYNTAX_ERROR.newException(s"${e.getMessage}\n[sql]\n${cmd}", e)

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

    def cmpMsg(op: String, l: Any, r: Any): String =
      (l, r) match
        case (l: Any, r: Any) if isShortString(l) && isShortString(r) =>
          s"${l} ${op} ${r}"
        case _ =>
          s"[left]\n${l}\n${op}\n[right]\n${r}"

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
