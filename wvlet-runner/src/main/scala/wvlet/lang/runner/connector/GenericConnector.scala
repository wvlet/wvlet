package wvlet.lang.runner.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType.Generic
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import java.sql.Connection

class GenericConnector(workEnv: WorkEnv) extends DBConnector(Generic, workEnv):
  private var _connector: DBConnector = null

  override def close(): Unit = Option(_connector).foreach(_.close())

  private def getConnector: DBConnector =
    if _connector == null then
      // Use DuckDB as a fallback connector, but initialize it only when necessary
      // as loading duckdb-jdbc takes a few seconds
      _connector = DuckDBConnector(workEnv)
    _connector

  override private[connector] def newConnection: DBConnection = getConnector.newConnection

  override def listFunctions(catalog: String): List[SQLFunction] = getConnector
    .listFunctions(catalog)
