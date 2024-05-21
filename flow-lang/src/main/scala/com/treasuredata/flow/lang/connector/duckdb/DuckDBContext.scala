package com.treasuredata.flow.lang.connector.duckdb

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.connector.DBContext

import java.sql.{Connection, SQLException}
import java.util.Properties

class DuckDBContext() extends DBContext:
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

end DuckDBContext
