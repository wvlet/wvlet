package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Compiler, Context}
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.SQLGenerator
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.log.{LogSupport, Logger}

import java.sql.DriverManager
import scala.util.Using

object DuckDBExecutor extends LogSupport:
  def default: DuckDBExecutor = DuckDBExecutor()

  def execute(sourceFolder: String, file: String): Unit =
    val result = Compiler.default.compile(sourceFolder)
    result
      .inFile(file).foreach: u =>
        execute(u, result.context)

  def execute(u: CompilationUnit, context: Context): Unit =
    val result       = default.execute(u.resolvedPlan, context)
    val resultString = QueryResultPrinter.print(result)
    debug(resultString)

class DuckDBExecutor extends LogSupport:
  def execute(plan: LogicalPlan, context: Context): QueryResult =
    plan match
      case p: PackageDef =>
        val results = p.statements.map { stmt =>
          PlanResult(stmt, execute(stmt, context))
        }
        QueryResultList(results)
      case q: Query =>
        val sql = SQLGenerator.toSQL(q)
        debug(s"Executing SQL:\n${sql}")
        val result = DuckDBDriver.withConnection: conn =>
          Using.resource(conn.createStatement()): stmt =>
            Using.resource(stmt.executeQuery(sql)): rs =>
              val codec       = JDBCCodec(rs)
              val recordCodec = MessageCodec.of[Map[String, Any]]
              val results: Seq[Map[String, Any]] = codec
                .mapMsgPackMapRows: msgpack =>
                  recordCodec.fromMsgPack(msgpack)
                .toSeq
              TableRows(q.relationType, results)
        result
      case t: TestDef =>
        debug(s"Executing test: ${t}")
        QueryResult.empty
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported plan: ${other}")

object DuckDBDriver:
  def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        try
          f(conn)
        finally
          conn.close()
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")
