package com.treasuredata.flow.lang.runner.connector.duckdb

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.sql.*
import com.treasuredata.flow.lang.runner.connector.DBContext
import org.duckdb.DuckDBConnection
import wvlet.airframe.metrics.ElapsedTime
import wvlet.log.LogSupport

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.util.Using

class DuckDBContext(prepareTPCH: Boolean = false)
    extends DBContext
    with AutoCloseable
    with LogSupport:

  // We need to reuse he same connection for preserving in-memory tables
  private var conn: DuckDBConnection = null

  private val initThread =
    new Thread(
      new Runnable:
        override def run(): Unit =
          val nano = System.nanoTime()
          logger.trace("Initializing DuckDB connection")
          conn = newConnection
          logger.trace(s"Finished initializing DuckDB. ${ElapsedTime.nanosSince(nano)}")
    )

  // Initialize DuckDB in the background thread as it may take several seconds
  init()

  private def init(): Unit = initThread.start

  override def newConnection: DuckDBConnection =
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

  override def close(): Unit = Option(conn).foreach { c =>
    c.close()
    conn = null
  }

  private def getConnection: DuckDBConnection =
    if conn == null && initThread.isAlive then
      // Wait until the connection is available
      initThread.join()

    if conn == null then
      throw StatusCode.NON_RETRYABLE_INTERNAL_ERROR.newException("Failed to initialize DuckDB")
    conn

  override def withConnection[U](body: Connection => U): U =
    try
      body(getConnection)
    catch
      case e: SQLException if e.getMessage.contains("403") =>
        throw StatusCode.PERMISSION_DENIED.newException(e.getMessage, e)

end DuckDBContext
