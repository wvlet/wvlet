package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.codegen.GenSQL
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Compiler, Context}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.SQLGenerator
import com.treasuredata.flow.lang.runner.DuckDBExecutor.default
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.sql.{DriverManager, SQLException}
import scala.collection.immutable.ListMap
import scala.util.Using

object DuckDBExecutor extends LogSupport:
  def default: DuckDBExecutor = DuckDBExecutor()

class DuckDBExecutor(prepareTPCH: Boolean = false) extends LogSupport with AutoCloseable:

  private val conn: DuckDBConnection =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        if prepareTPCH then
          Using.resource(conn.createStatement()): stmt =>
            stmt.execute("install tpch")
            stmt.execute("load tpch")
            stmt.execute("call dbgen(sf = 0.01)")
        conn
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

  override def close(): Unit =
    try
      conn.close()
    catch
      case e: Throwable =>
        warn(e)

  def execute(sourceFolder: String, file: String): Unit =
    val result = Compiler.default.compile(sourceFolder)
    result
      .inFile(file)
      .foreach: u =>
        execute(u, result.context)

  def execute(u: CompilationUnit, context: Context): QueryResult =
    val result = execute(u.resolvedPlan, context)
    result

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
        val generatedSQL = GenSQL.generateSQL(q, context)
        debug(s"Executing SQL:\n${generatedSQL.sql}")
        try
          val result =
            Using.resource(conn.createStatement()) { stmt =>
              Using.resource(stmt.executeQuery(generatedSQL.sql)) { rs =>
                val codec       = JDBCCodec(rs)
                val resultCodec = MessageCodec.of[Seq[ListMap[String, Any]]]
                val results     = resultCodec.fromMsgPack(codec.toMsgPack)
                TableRows(generatedSQL.plan.relationType, results)
              }
            }

          if logger.isEnabled(LogLevel.TRACE) then
            val resultString = QueryResultPrinter.print(result, limit = Some(10))
            trace(resultString)
          result
        catch
          case e: SQLException =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Failed to execute SQL: ${generatedSQL.sql}\n${e.getMessage}", e)
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
