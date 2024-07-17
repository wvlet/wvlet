package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.codegen.GenSQL
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Compiler, Context}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.SQLGenerator
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.sql.{DriverManager, SQLException}
import scala.util.Using

object DuckDBExecutor extends LogSupport:
  def default: DuckDBExecutor = DuckDBExecutor()

  def execute(sourceFolder: String, file: String): Unit =
    val result = Compiler.default.compile(sourceFolder)
    result
      .inFile(file)
      .foreach: u =>
        execute(u, result.context)

  def execute(u: CompilationUnit, context: Context): QueryResult =
    val result = default.execute(u.resolvedPlan, context)
    result

class DuckDBExecutor extends LogSupport:
  def execute(plan: LogicalPlan, context: Context): QueryResult =
    plan match
      case p: PackageDef =>
        val results = p
          .statements
          .map { stmt =>
            PlanResult(stmt, execute(stmt, context))
          }
        QueryResultList(results)
      case q: Query =>
        val sql = GenSQL.generateSQL(q, context)
        debug(s"Executing SQL:\n${sql}")
        try
          val result = DuckDBDriver.withConnection { conn =>
            Using.resource(conn.createStatement()) { stmt =>
              Using.resource(stmt.executeQuery(sql)) { rs =>
                val codec       = JDBCCodec(rs)
                val resultCodec = MessageCodec.of[Seq[Map[String, Any]]]
                val results     = resultCodec.fromMsgPack(codec.toMsgPack)
                TableRows(q.relationType, results)
              }
            }
          }
          if logger.isEnabled(LogLevel.TRACE) then
            val resultString = QueryResultPrinter.print(result, limit = Some(10))
            trace(resultString)
          result
        catch
          case e: SQLException =>
            throw StatusCode.INVALID_ARGUMENT.newException(s"Failed to execute SQL: ${sql}", e)
      case t: TableDef =>
        QueryResult.empty
      case t: TestRelation =>
        debug(s"Executing test: ${t}")
        QueryResult.empty
      case m: ModelDef =>
        QueryResult.empty
      case s: Subscribe =>
        debug(s"Executing subscribe: ${s}")
        QueryResult.empty
      case f: LanguageStatement =>
        QueryResult.empty
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported plan: ${other}")

end DuckDBExecutor

object DuckDBDriver:
  def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        try f(conn)
        finally conn.close()
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")
