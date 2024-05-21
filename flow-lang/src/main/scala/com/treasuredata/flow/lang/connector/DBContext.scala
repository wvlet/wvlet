package com.treasuredata.flow.lang.connector

import com.treasuredata.flow.lang.connector.common.exprs.Expr
import wvlet.airframe.control.Control.withResource

import java.sql.Connection

object DBContext:
  case class Table(catalog: String, schema: String, table: String)
  case class Schema(catalog: String, schema: String)

import com.treasuredata.flow.lang.connector.DBContext.*

trait DBContext extends AutoCloseable:
  private var _self: Expr[Any] = _
  def self: Expr[Any]          = ???
  def withSelf(self: Expr[Any]): this.type =
    _self = self
    this

  protected def newConnection: Connection

  def withConnection[U](body: Connection => U): U =
    val conn = newConnection
    try body(conn)
    finally conn.close()

  def getTable(catalog: String, schema: String, table: String): Option[Table] =
    val foundTables = Seq.newBuilder[DBContext.Table]

    withConnection: conn =>
      val rs = conn
        .createStatement().executeQuery(
          s"""select * from information_schema.tables
             |where table_catalog = '${catalog}' and table_schema = '${schema}' and table_name = '${table}'
             |""".stripMargin
        )
      while rs.next() do
        foundTables += DBContext.Table(
          catalog = rs.getString("table_catalog"),
          schema = rs.getString("table_schema"),
          table = rs.getString("table_name")
        )

    foundTables.result().headOption

  def getSchema(catalog: String, schema: String): Option[Schema] =
    val foundSchemas = Seq.newBuilder[DBContext.Schema]

    withConnection: conn =>
      withResource(
        conn
          .createStatement().executeQuery(
            s"""select * from information_schema.schemata
             |where catalog_name = '${catalog}' and schema_name = '${schema}'
             |""".stripMargin
          )
      ) { rs =>
        while rs.next() do
          foundSchemas += DBContext.Schema(
            catalog = rs.getString("catalog_name"),
            schema = rs.getString("schema_name")
          )
      }

    foundSchemas.result().headOption

  def createSchema(catalog: String, schema: String): DBContext.Schema =
    withConnection: conn =>
      conn.createStatement().executeUpdate(s"""create schema if not exists ${catalog}.${schema}""")
      DBContext.Schema(catalog, schema)

  def dropTable(catalog: String, schema: String, table: String): Unit =
    withConnection: conn =>
      conn.createStatement().executeUpdate(s"""drop table if exists ${catalog}.${schema}.${table}""")

  def dropSchema(catalog: String, schema: String): Unit =
    withConnection: conn =>
      conn.createStatement().executeUpdate(s"""drop schema if exists ${catalog}.${schema}""")
