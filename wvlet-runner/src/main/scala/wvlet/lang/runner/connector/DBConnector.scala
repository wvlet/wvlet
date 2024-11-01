/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.connector

import wvlet.airframe.control.Control.withResource
import DBConnector.*
import wvlet.lang.catalog.{Catalog, SQLFunction}
import wvlet.lang.catalog.Catalog.{TableColumn, TableName, TableSchema}
import wvlet.lang.compiler.{DBType, Name}
import wvlet.lang.model.DataType.{NamedType, SchemaType}
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.*
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.log.LogSupport

import java.sql.{Connection, ResultSet, SQLWarning}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

object DBConnector:
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

trait DBConnector(val dbType: DBType) extends AutoCloseable with LogSupport:
  private var queryScope: QueryScope = QueryScope.Global

  private val catalogs = new ConcurrentHashMap[String, ConnectorCatalog]().asScala

  def withQueryScope(scope: QueryScope): this.type =
    queryScope = scope
    this

  protected def newConnection: Connection

  def getCatalog(catalogName: String, defaultSchema: String): Catalog = catalogs.getOrElseUpdate(
    catalogName,
    new ConnectorCatalog(
      catalogName = catalogName,
      defaultSchema = defaultSchema,
      dbConnector = this
    )
  )

  def withConnection[U](body: Connection => U): U =
    val conn = newConnection
    try body(conn)
    finally conn.close()

  def runQuery[U](sql: String)(handler: ResultSet => U): U = withConnection: conn =>
    trace(s"Running SQL: ${sql}")
    withResource(conn.createStatement()): stmt =>
      withResource(stmt.executeQuery(sql)): rs =>
        handler(rs)

  def executeUpdate(sql: String): Int = withConnection: conn =>
    withResource(conn.createStatement()): stmt =>
      stmt.executeUpdate(sql)

  def processWarning(w: java.sql.SQLWarning): Unit =
    def showWarnings(w: SQLWarning): Unit =
      w match
        case null =>
        case _ =>
          warn(w.getMessage)
          showWarnings(w.getNextWarning)
    showWarnings(w)

  def getCatalogNames: List[String] = withConnection: conn =>
    withResource(conn.getMetaData().getCatalogs()): rs =>
      val catalogs = List.newBuilder[String]
      while rs.next() do
        catalogs += rs.getString("TABLE_CAT")
      catalogs.result()

  private def toTableDef(
      catalog: String,
      schema: String,
      table: String,
      columns: Seq[JDBCColumn]
  ): Catalog.TableDef =
    val fields = columns
      .sortBy(c => c.ordinal_position)
      .map { c =>
        TableColumn(c.column_name, DataType.parse(c.data_type))
      }
    Catalog.TableDef(TableName(Some(catalog), Some(schema), table), columns = fields)

  def listTableDefs(catalog: String, schema: String): List[Catalog.TableDef] =
    val columns = List.newBuilder[JDBCColumn]
    runQuery(
      s"""select table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type
             |from information_schema.columns
             |where table_catalog = '${catalog}' and table_schema = '${schema}'""".stripMargin
    ): rs =>
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
        toTableDef(catalog, schema, table, cols)
      }

    schemas.toList

  end listTableDefs

  def getTableDef(catalog: String, schema: String, table: String): Option[Catalog.TableDef] =
    val columns = List.newBuilder[JDBCColumn]
    runQuery(
      s"""select table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type
             |from information_schema.columns
             |where table_catalog = '${catalog}' and table_schema = '${schema}' and table_name = '${table}'
             |""".stripMargin
    ): rs =>
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
        toTableDef(catalog, schema, table, cols)
      }

    schemas.headOption

  end getTableDef

  def listTables(catalog: String, schema: String): List[TableName] =
    val foundTables = List.newBuilder[TableName]
    runQuery(s"""select * from information_schema.tables
             |where table_catalog = '${catalog}' and table_schema = '${schema}'""".stripMargin) {
      rs =>
        while rs.next() do
          foundTables +=
            TableName(
              catalog = Option(rs.getString("table_catalog")),
              schema = Option(rs.getString("table_schema")),
              name = rs.getString("table_name")
            )
    }
    foundTables.result()

  def listSchemaNames(catalog: String): List[String] = listSchemas(catalog).map(_.name)

  def listSchemas(catalog: String): List[TableSchema] =
    val foundSchemas = List.newBuilder[TableSchema]
    runQuery(
      s"""select schema_name from information_schema.schemata where catalog_name = '${catalog}'"""
    ) { rs =>
      while rs.next() do
        foundSchemas += TableSchema(catalog = Some(catalog), name = rs.getString(1))
    }
    foundSchemas.result()

  def getSchema(catalog: String, schema: String): Option[TableSchema] =
    val foundSchemas = Seq.newBuilder[TableSchema]
    runQuery(s"""select * from information_schema.schemata
             |where catalog_name = '${catalog}' and schema_name = '${schema}'
             |""".stripMargin) { rs =>
      while rs.next() do
        foundSchemas += TableSchema(catalog = Some(catalog), name = rs.getString("schema_name"))
    }

    foundSchemas.result().headOption

  def createSchema(catalog: String, schema: String): TableSchema =
    executeUpdate(s"""create schema if not exists ${catalog}.${schema}""")
    TableSchema(Some(catalog), schema)

  def dropTable(catalog: String, schema: String, table: String): Unit = executeUpdate(
    s"""drop table if exists ${catalog}.${schema}.${table}"""
  )

  def dropSchema(catalog: String, schema: String): Unit = executeUpdate(
    s"""drop schema if exists ${catalog}.${schema}"""
  )

  def listFunctions(catalog: String): List[SQLFunction]

end DBConnector
