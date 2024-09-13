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

import wvlet.lang.StatusCode
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
    workEnv: WvletWorkEnv,
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
      CompilerOptions(sourceFolders = List(sourceFolder), workingFolder = sourceFolder)
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

    def process(e: ExecutionPlan): QueryResult =
      e match
        case ExecuteQuery(plan) =>
          lastResult = executeQuery(plan, context)
          lastResult
        case ExecuteTest(test) =>
          lastResult = executeTest(test, context, lastResult)
          lastResult
        case ExecuteTasks(tasks) =>
          val results = tasks.map { task =>
            process(task)
          }
          lastResult = QueryResult.fromList(results)
          lastResult
        case ExecuteCommand(e) =>
          // Command produces no QueryResult other than errors
          executeCommand(e.expr, context)
        case ExecuteNothing =>
          QueryResult.empty

    process(executionPlan)

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

  private def executeTest(
      test: TestRelation,
      context: Context,
      lastResult: QueryResult
  ): QueryResult =

    def eval(e: Expression): QueryResult =
      e match
        case ShouldExpr(TestType.ShouldBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue != rightValue then
            val msg = s"match failure:\n[left]\n${leftValue}\n\n[right]\n${rightValue}"
            workEnv.errorLogger.error(msg)
            ErrorResult(StatusCode.TEST_FAILED.newException(msg))
          else
            workEnv
              .outLogger
              .debug(s"match success:\n[left]\n${leftValue}\n\n[right]\n${rightValue}")
            QueryResult.empty
        case _ =>
          WarningResult(s"Unsupported test expression: ${e}")

    def evalOp(e: Expression): Any =
      e match
        case DotRef(c: ContextInputRef, name, _, _) if name.leafName == "output" =>
          val box = lastResult.toPrettyBox()
          box
        case l: StringLiteral =>
          l.value
        case _ =>
          ()

    def trim(v: Any): Any =
      v match
        case s: String =>
          s.trim
        case _ =>
          v

    eval(test.testExpr)

  end executeTest

end QueryExecutor
