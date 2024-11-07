package wvlet.lang.runner.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType.Generic
import wvlet.lang.runner.connector.duckdb.DuckDBConnector

import java.sql.Connection

class GenericConnector extends DBConnector(Generic):
  private var connector: DBConnector = null

  override def close(): Unit = Option(connector).foreach(_.close())

  override def newConnection: Connection =
    if connector == null then
      // Use DuckDB as a fallback connector, but initialize it only when necessary
      // as loading duckdb-jdbc takes a few seconds
      connector = DuckDBConnector()
    connector.newConnection

  override def listFunctions(catalog: String): List[SQLFunction] = Nil
