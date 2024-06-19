package com.treasuredata.flow.lang.connector

import wvlet.airframe.control.Control.withResource
import com.treasuredata.flow.lang.connector.DBContext.*
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.SqlExpr
import com.treasuredata.flow.lang.model.sql.SqlExpr.*

import java.sql.Connection

object DBContext:
  case class Table(catalog: String, schema: String, table: String)
  case class Schema(catalog: String, schema: String)

enum QueryScope:
  case Global,
    InQuery,
    InExpr

trait DBContext extends AutoCloseable:
  private var _self: SqlExpr     = _
  private var _plan: LogicalPlan = _

  private var queryScope: QueryScope = QueryScope.Global

  def self: SqlExpr = _self

  def plan: LogicalPlan = ???
  def withSelf(newSelf: SqlExpr): this.type =
    _self = newSelf
    this

  def withPlan(plan: LogicalPlan): this.type =
    _plan = plan
    this

  def withQueryScope(scope: QueryScope): this.type =
    queryScope = scope
    this

  protected def newConnection: Connection

  def IString: IString
  def IBoolean: IBoolean
  def IInt: IInt
  def ILong: ILong
  def IFloat: IFloat
  def IDouble: IDouble

  def withConnection[U](body: Connection => U): U =
    val conn = newConnection
    try body(conn)
    finally conn.close()

  def getTable(catalog: String, schema: String, table: String): Option[Table] =
    val foundTables = Seq.newBuilder[DBContext.Table]

    withConnection: conn =>
      val rs = conn
        .createStatement()
        .executeQuery(s"""select * from information_schema.tables
             |where table_catalog = '${catalog}' and table_schema = '${schema}' and table_name = '${table}'
             |""".stripMargin)
      while rs.next() do
        foundTables +=
          DBContext.Table(
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
          .createStatement()
          .executeQuery(s"""select * from information_schema.schemata
             |where catalog_name = '${catalog}' and schema_name = '${schema}'
             |""".stripMargin)
      ) { rs =>
        while rs.next() do
          foundSchemas +=
            DBContext
              .Schema(catalog = rs.getString("catalog_name"), schema = rs.getString("schema_name"))
      }

    foundSchemas.result().headOption

  def createSchema(catalog: String, schema: String): DBContext.Schema = withConnection: conn =>
    conn.createStatement().executeUpdate(s"""create schema if not exists ${catalog}.${schema}""")
    DBContext.Schema(catalog, schema)

  def dropTable(catalog: String, schema: String, table: String): Unit = withConnection: conn =>
    conn.createStatement().executeUpdate(s"""drop table if exists ${catalog}.${schema}.${table}""")

  def dropSchema(catalog: String, schema: String): Unit = withConnection: conn =>
    conn.createStatement().executeUpdate(s"""drop schema if exists ${catalog}.${schema}""")
