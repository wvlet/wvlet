package wvlet.lang.runner.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType.Generic
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import java.sql.Connection

class GenericConnector extends DBConnector(Generic):
  private var _connector: DBConnector = null

  override def close(): Unit = Option(_connector).foreach(_.close())

  private def getConnector: DBConnector =
    if _connector == null then
      // Use DuckDB as a fallback connector, but initialize it only when necessary
      // as loading duckdb-jdbc takes a few seconds
      _connector = DuckDBConnector()
    _connector

  override def newConnection: Connection = getConnector.newConnection

  override def listFunctions(catalog: String): List[SQLFunction] = getConnector
    .listFunctions(catalog)
