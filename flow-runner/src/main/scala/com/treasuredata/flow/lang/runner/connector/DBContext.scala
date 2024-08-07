package com.treasuredata.flow.lang.runner.connector

import wvlet.airframe.control.Control.withResource
import DBContext.*
import com.treasuredata.flow.lang.compiler.Name
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType}
import com.treasuredata.flow.lang.model.{DataType, RelationType}
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import com.treasuredata.flow.lang.model.plan.*
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.log.LogSupport

import java.sql.{Connection, SQLWarning}

object DBContext:
  case class Table(catalog: String, schema: String, table: String)
  case class Schema(catalog: String, schema: String)

  private case class JDBCColumn(
      table_catalog: String,
      table_schema: String,
      table_name: String,
      column_name: String,
      ordinal_position: Int,
      is_nullable: String,
      data_type: String
  )

enum QueryScope:
  case Global,
    InQuery,
    InExpr

trait DBContext extends AutoCloseable with LogSupport:
  private var _plan: LogicalPlan     = _
  private var queryScope: QueryScope = QueryScope.Global

  def plan: LogicalPlan = ???

  def withPlan(plan: LogicalPlan): this.type =
    _plan = plan
    this

  def withQueryScope(scope: QueryScope): this.type =
    queryScope = scope
    this

  protected def newConnection: Connection

  def withConnection[U](body: Connection => U): U =
    val conn = newConnection
    try body(conn)
    finally conn.close()

  def processWarning(w: java.sql.SQLWarning): Unit =
    def showWarnings(w: SQLWarning): Unit =
      w match
        case null =>
        case _ =>
          warn(w.getMessage)
          showWarnings(w.getNextWarning)
    showWarnings(w)

  def getTableDefs(catalog: String, schema: String): List[SchemaType] = withConnection: conn =>
    val rs = conn
      .createStatement()
      .executeQuery(
        s"""select table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type
             |from information_schema.columns
             |where table_catalog = '${catalog}' and table_schema = '${schema}'""".stripMargin
      )
    val columns = List.newBuilder[JDBCColumn]
    while rs.next() do
      columns +=
        JDBCColumn(
          table_catalog = rs.getString("table_catalog"),
          table_schema = rs.getString("table_schema"),
          table_name = rs.getString("table_name"),
          column_name = rs.getString("column_name"),
          ordinal_position = rs.getInt("ordinal_position"),
          is_nullable = rs.getString("is_nullable"),
          data_type = rs.getString("data_type").toLowerCase
        )

    val schemas = columns
      .result()
      .groupBy(c => (c.table_catalog, c.table_schema, c.table_name))
      .map { case ((catalog, schema, table), cols) =>
        val fields = cols
          .sortBy(c => c.ordinal_position)
          .map { c =>
            NamedType(Name.termName(c.column_name), DataType.parse(c.data_type))
          }
        SchemaType(
          // TODO Manage catalog, schema hierarchies
          None,
          Name.typeName(table),
          fields
        )
      }

    schemas.toList

  end getTableDefs

  def getTables(catalog: String, schema: String): List[Table] =
    val foundTables = List.newBuilder[DBContext.Table]
    withConnection: conn =>
      val rs = conn
        .createStatement()
        .executeQuery(s"""select * from information_schema.tables
             |where table_catalog = '${catalog}' and table_schema = '${schema}'""".stripMargin)
      while rs.next() do
        foundTables +=
          DBContext.Table(
            catalog = rs.getString("table_catalog"),
            schema = rs.getString("table_schema"),
            table = rs.getString("table_name")
          )
    foundTables.result()

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

end DBContext
