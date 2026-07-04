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
package wvlet.lang.runner.connector.trino

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableColumn
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.catalog.Catalog.TableSchema
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.analyzer.trino.TrinoConfig as TrinoXPConfig
import wvlet.lang.compiler.analyzer.trino.TrinoSqlConnector
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.DataType
import wvlet.lang.runner.connector.*
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver
import wvlet.uni.weaver.codec.PrimitiveWeaver.given

import java.sql.Statement
import scala.collection.immutable.ListMap

case class TrinoConfig(
    catalog: String,
    schema: String,
    hostAndPort: String,
    useSSL: Boolean = true,
    user: Option[String] = None,
    password: Option[String] = None
)

/**
  * JVM `DBConnector` for Trino that no longer uses `trino-jdbc`. All SQL flows through the
  * cross-platform [[TrinoSqlConnector]] (uni's `HttpSyncClient`). The `DBConnector` superclass is
  * still extended because callers (in particular `DBConnectorProvider` and `ConnectorCatalog`)
  * dispatch on that type today, but every JDBC-shaped method (`runQuery`, `executeUpdate`,
  * `withStatement`, `newConnection`) either delegates to HTTP or throws — none of them touch
  * `java.sql.*` machinery anymore. The base class's JDBC `getCatalog` plumbing is bypassed by
  * overriding the information-schema metadata methods (`listSchemas`, `listTables`, `getTableDef`,
  * `getCatalogNames`) with HTTP equivalents.
  *
  * `password` on [[TrinoConfig]] is preserved so the case class binary doesn't break clients
  * mid-migration, but the HTTP path uses `X-Trino-User` only today; Basic / JWT auth lands later.
  */
class TrinoConnector(val config: TrinoConfig, workEnv: WorkEnv)
    extends DBConnector(DBType.Trino, workEnv)
    with LogSupport:

  lazy val asSqlConnector: SqlConnector = TrinoSqlConnector(toCrossPlatformConfig)

  // Metadata methods (listSchemas, listTables, getTableDef, …) don't take a caller-supplied
  // QueryProgressMonitor because the base `DBConnector` doesn't either — so we feed the
  // cross-platform connector a no-op monitor by default. Overrides that DO take a monitor
  // (`executeUpdate`, `execute`) shadow this default at the method level.
  private given defaultMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp

  private def toCrossPlatformConfig: TrinoXPConfig =
    val parts        = config.hostAndPort.split(":", 2)
    val host         = parts(0)
    val explicitPort =
      if parts.length > 1 then
        Some(parts(1).toInt)
      else
        None
    val defaultPortFor =
      if config.useSSL then
        443
      else
        8080
    TrinoXPConfig(
      host = host,
      port = explicitPort.getOrElse(defaultPortFor),
      user = config.user.getOrElse("wvlet"),
      catalog = Some(config.catalog),
      schema = Some(config.schema),
      useHttps = config.useSSL,
      source = "wvlet-runner"
    )

  /**
    * Run a SQL string over the HTTP path and return the materialized [[QueryResult]]. Centralises
    * the `asSqlConnector.execute` call so the overrides below stay one-liners. Takes the progress
    * monitor implicitly so callers that already have one (e.g. `execute` / `executeUpdate`) can
    * thread it through, while metadata callers fall back to the class-level `noOp` default.
    *
    * Note: `SqlConnector.execute` materializes the full result set today — same tradeoff PR-B
    * shipped for `QueryExecutor`'s Trino path. Large exports that previously streamed through the
    * JDBC `ResultSet` now build the whole `QueryResult` in memory. Adding chunked iteration to
    * `QueryHandle` is the right fix; tracked as a follow-up.
    */
  private def http(sql: String)(using QueryProgressMonitor): QueryResult = asSqlConnector.execute(
    sql
  )

  /** SQL-safe literal escaping for the values we interpolate into information_schema queries. */
  private def lit(s: String): String = s"'${s.replace("'", "''")}'"

  override def close(): Unit = ()

  def withConfig(newConfig: TrinoConfig): TrinoConnector = new TrinoConnector(newConfig, workEnv)

  // -------- JDBC-shaped abstract members of DBConnector ----------------------------------------
  //
  // These exist only because the base class declares them. Every caller has either been moved to
  // HTTP (`asSqlConnector`) or is exercised through overridden metadata methods. If a future caller
  // slips through and triggers `newConnection`, the throw below is the loud failure mode we want.

  private[connector] override def newConnection: DBConnection =
    throw new UnsupportedOperationException(
      "TrinoConnector no longer uses JDBC. Use `asSqlConnector` or the connector's metadata APIs."
    )

  override protected def withConnection[U](body: DBConnection => U): U = newConnection.asInstanceOf[
    Nothing
  ]

  override protected def withStatement[U](body: Statement => U)(using
      queryProgressMonitor: QueryProgressMonitor
  ): U = newConnection.asInstanceOf[Nothing]

  // -------- DDL / mutation overrides -----------------------------------------------------------

  override def executeUpdate(sql: String)(using QueryProgressMonitor): Int =
    http(sql)
    // Trino's HTTP API doesn't surface an "affected rows" count for DDL/DML the way JDBC
    // `Statement.executeUpdate` does (the `updateCount` in stats is best-effort and not always
    // populated). Return 0 to match the historical wvlet usage — every existing caller treats
    // this as a fire-and-forget operation.
    0

  override def execute(sql: String)(using monitor: QueryProgressMonitor): Boolean =
    // Match the base class's monitor lifecycle so REPL/UI progress UI stays in sync —
    // base impl calls `newQuery` before, `close` after.
    monitor.newQuery(sql)
    try http(sql).columnCount > 0
    finally monitor.close()

  /**
    * Flow statements run through the HTTP query handle, whose `cancel()` issues a DELETE on the
    * query so that stage timeouts and cancellations stop the query server-side, mirroring what JDBC
    * `Statement.cancel` provides on other engines
    */
  override private[runner] def executeCancellable(
      sql: String,
      register: CancellableStatement => Unit,
      deregister: () => Unit
  ): Unit =
    val handle = asSqlConnector.submit(sql)
    try
      register(() => handle.cancel())
      val _ = handle.await()
    finally
      deregister()
      handle.close()

  override private[runner] def queryJsonRows(sql: String): List[String] =
    val result    = http(sql)
    val names     = result.columns.map(_.name.name)
    val rowWeaver = summon[Weaver[ListMap[String, Any]]]
    result
      .rows
      .map(row => rowWeaver.toJson(ListMap.from(names.zip(row.values.map(v => v.orNull: Any)))))
      .toList

  override def createSchema(catalog: String, schema: String): TableSchema =
    http(s"create schema if not exists ${catalog}.${schema}")
    TableSchema(Some(catalog), schema)

  override def dropTable(catalog: String, schema: String, table: String): Unit = http(
    s"drop table if exists ${catalog}.${schema}.${table}"
  )

  override def dropSchema(catalog: String, schema: String): Unit = http(
    s"drop schema if exists ${catalog}.${schema}"
  )

  // -------- Metadata overrides (information_schema over HTTP) ----------------------------------

  override def getCatalogNames: List[String] =
    val r = http("show catalogs")
    r.rows.flatMap(_.values.headOption.flatten).toList

  override def listSchemas(catalog: String): List[TableSchema] =
    val r = http(
      s"select schema_name from information_schema.schemata where catalog_name = ${lit(catalog)}"
    )
    r.rows.flatMap(_.values.headOption.flatten).map(name => TableSchema(Some(catalog), name)).toList

  override def listSchemaNames(catalog: String): List[String] = listSchemas(catalog).map(_.name)

  override def getSchema(catalog: String, schema: String): Option[TableSchema] = listSchemas(
    catalog
  ).find(_.name == schema)

  override def listTables(catalog: String, schema: String): List[TableName] =
    val r = http(
      s"select table_name from information_schema.tables where table_catalog = ${lit(
          catalog
        )} and table_schema = ${lit(schema)}"
    )
    r.rows
      .flatMap(_.values.headOption.flatten)
      .map(name => TableName(Some(catalog), Some(schema), name))
      .toList

  override def listTableDefs(catalog: String, schema: String): List[Catalog.TableDef] =
    val r = http(s"""select table_name, column_name, ordinal_position, data_type
         |from information_schema.columns
         |where table_catalog = ${lit(catalog)} and table_schema = ${lit(schema)}""".stripMargin)
    val idx = columnIndex(r)
    r.rows
      .groupBy(row => stringAt(row, idx, "table_name").getOrElse(""))
      .map { case (table, rows) =>
        val columns = rows
          .sortBy(row => stringAt(row, idx, "ordinal_position").flatMap(_.toIntOption).getOrElse(0))
          .map { row =>
            TableColumn(
              stringAt(row, idx, "column_name").getOrElse(""),
              DataType.parse(stringAt(row, idx, "data_type").getOrElse("any").toLowerCase)
            )
          }
        Catalog.TableDef(TableName(Some(catalog), Some(schema), table), columns = columns)
      }
      .toList

  override def getTableDef(
      catalog: String,
      schema: String,
      table: String
  ): Option[Catalog.TableDef] =
    val r = http(
      s"""select column_name, ordinal_position, data_type
         |from information_schema.columns
         |where table_catalog = ${lit(catalog)} and table_schema = ${lit(
          schema
        )} and table_name = ${lit(table)}""".stripMargin
    )
    if r.rowCount == 0 then
      None
    else
      val idx     = columnIndex(r)
      val columns = r
        .rows
        .sortBy(row => stringAt(row, idx, "ordinal_position").flatMap(_.toIntOption).getOrElse(0))
        .map { row =>
          TableColumn(
            stringAt(row, idx, "column_name").getOrElse(""),
            DataType.parse(stringAt(row, idx, "data_type").getOrElse("any").toLowerCase)
          )
        }
      Some(Catalog.TableDef(TableName(Some(catalog), Some(schema), table), columns = columns))

  /**
    * Trino's `show functions` over HTTP. Result columns are addressed by name rather than position
    * because Trino reorders them between versions (e.g. 481 added a `Variadic` column); a
    * positional lookup would silently break under those upgrades.
    */
  override def listFunctions(catalog: String): List[SQLFunction] =
    val result = http("show functions")
    val idx    = columnIndex(result)
    result
      .rows
      .iterator
      .map { row =>
        val returnType = stringAt(row, idx, "Return Type")
          .map(DataType.parse)
          .getOrElse(DataType.AnyType)
        // Zero-arg functions (e.g. `now()`, `pi()`) report an empty `Argument Types` cell.
        // `"".split(", ")` returns `Array("")` in Scala — guard before parsing.
        val argumentTypes = stringAt(row, idx, "Argument Types")
          .filter(_.nonEmpty)
          .map(_.split(", ").iterator.map(DataType.parse).toList)
          .getOrElse(Nil)
        val prop = Map.newBuilder[String, Any]
        stringAt(row, idx, "description").foreach(x => prop += "description" -> x)
        SQLFunction(
          name = stringAt(row, idx, "Function").getOrElse(""),
          functionType = SQLFunction
            .FunctionType
            .valueOf(stringAt(row, idx, "Function Type").getOrElse("scalar").toUpperCase),
          returnType = returnType,
          args = argumentTypes,
          properties = prop.result()
        )
      }
      .toList

  // -------- helpers ----------------------------------------------------------------------------

  private def columnIndex(r: QueryResult): Map[String, Int] =
    r.columns.map(_.name.name.toLowerCase).zipWithIndex.toMap

  private def stringAt(
      row: wvlet.lang.compiler.connector.QueryResultRow,
      idx: Map[String, Int],
      name: String
  ): Option[String] = idx.get(name.toLowerCase).flatMap(row.values.lift).flatten

end TrinoConnector
