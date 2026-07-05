package wvlet.lang.connector.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType.Generic
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.DBConnection
import wvlet.lang.connector.DBConnector

import java.sql.Connection

class GenericConnector(workEnv: WorkEnv) extends DBConnector(Generic, workEnv):
  private var _connector: DuckDBConnector = null

  override def close(): Unit = Option(_connector).foreach(_.close())

  private def getConnector: DuckDBConnector =
    if _connector == null then
      // Use DuckDB as a fallback connector, but initialize it only when necessary
      // as loading duckdb-jdbc takes a few seconds
      _connector = DuckDBConnector(workEnv)
    _connector

  override private[connector] def newConnection: DBConnection = getConnector.newConnection

  // Delegate to DuckDBConnector, which reuses a single connection. This preserves
  // connection-scoped state (in-memory and temp tables) across statements, which flow stage
  // materialization and multi-statement scripts rely on
  override protected def withConnection[U](body: DBConnection => U): U = getConnector
    .withConnection(body)

  override private[lang] def newSession: DBConnection = getConnector.newSession

  override def listFunctions(catalog: String): List[SQLFunction] = getConnector.listFunctions(
    catalog
  )
