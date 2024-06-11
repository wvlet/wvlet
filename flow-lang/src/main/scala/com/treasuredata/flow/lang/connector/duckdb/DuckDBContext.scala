package com.treasuredata.flow.lang.connector.duckdb

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.connector.DBContext
import com.treasuredata.flow.lang.model.sql.*
import com.treasuredata.flow.lang.model.sql.SqlExpr
import com.treasuredata.flow.lang.model.sql.SqlExpr.ExprInterpreter

import java.sql.{Connection, SQLException}
import java.util.Properties

class DuckDBContext extends DBContext:
  Class.forName("org.duckdb.DuckDBDriver")

  override def newConnection =
    val prop = new Properties()
    java.sql.DriverManager.getConnection("jdbc:duckdb:", prop)

  // Reuse the same connection for preserving in-memory tables
  private lazy val conn = newConnection

  override def close(): Unit =
    conn.close()

  override def withConnection[U](body: Connection => U): U =
    try body(conn)
    catch
      case e: SQLException if e.getMessage.contains("403") =>
        throw StatusCode.PERMISSION_DENIED.newException(e.getMessage, e)

  override def IString: IString = DuckDBString(using this)

  class DuckDBString(using ctx: DuckDBContext) extends IString:
    override def toInt     = sql"${self}::integer"
    override def toLong    = sql"${self}::long"
    override def toFloat   = sql"${self}::float"
    override def toDouble  = sql"${self}::double"
    override def toBoolean = sql"${self}::boolean"
    override def length    = sql"length(${self})"

    override def substring(start: SqlExpr): SqlExpr = sql"substring(${self}, ${start}, strlen(${self}))"
    override def substring(start: SqlExpr, end: SqlExpr): SqlExpr =
      sql"substring(${self}, ${start}, ${end})"

    override def regexpContains(pattern: SqlExpr): SqlExpr = sql"regexp_matches(${self}, ${pattern})"
  end DuckDBString

  class DuckDBBoolean(using ctx: DuckDBContext) extends IBoolean:
    override def unary_! : SqlExpr = sql"not ${self}"

    override def ==(x: SqlExpr): SqlExpr  = sql"${self} == ${x}"
    override def !=(x: SqlExpr): SqlExpr  = sql"${self} != ${x}"
    override def and(x: SqlExpr): SqlExpr = sql"${self} and ${x}"
    override def or(x: SqlExpr): SqlExpr  = sql"${self} or ${x}"
    override def &&(x: SqlExpr): SqlExpr  = and(x)
    override def ||(x: SqlExpr): SqlExpr  = or(x)
  end DuckDBBoolean

  class DuckDBInt(using ctx: DuckDBContext) extends IInt:
    override def toBoolean: SqlExpr = sql"${self}::boolean"
    override def toLong: SqlExpr    = sql"${self}::long"
    override def toFloat: SqlExpr   = sql"${self}::float"
    override def toDouble: SqlExpr  = sql"${self}::double"
    override def toStr: SqlExpr     = sql"${self}::string"
  end DuckDBInt

end DuckDBContext
