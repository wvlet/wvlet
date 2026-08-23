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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.InMemoryCatalog
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.SqlConnectorProvider

/**
  * The cross-platform `ExecutionPlan` interpreter: runs queries, multi-statement scripts, `test`
  * statements, `val` definitions, and `use` switching over [[SqlConnector]] engines (session-backed
  * DuckDB, Trino over REST) on JVM, Node.js, and Native alike. JVM-only plan nodes (save-to files,
  * flows) inherit [[BasePlanExecutor]]'s not-supported error.
  *
  * `use <connector>` switches the execution engine AND the SQL dialect of subsequent statements
  * (SQL is generated at execution time). Unlike the JVM `QueryExecutor`, no live catalog metadata
  * is registered — the thin CLIs compile catalog-free, so the switched engine gets an in-memory
  * catalog carrying its dialect and catalog/schema names.
  */
class PlanExecutor(
    connectorProvider: SqlConnectorProvider,
    defaultProfile: Profile,
    workEnv: WorkEnv,
    rowLimit: Int = 40
) extends BasePlanExecutor(workEnv):

  // The engine statements execute on: the profile's default engine until `use <connector>`
  // switches it. Session-level state — one PlanExecutor per session.
  private var activeConfig: ConnectorConfig = defaultProfile.defaultEngine

  private def activeConnector: SqlConnector = connectorProvider.getConnector(activeConfig)

  // The connector provider (and its cached connections) is owned by the caller
  override def close(): Unit = ()

  override protected def executeQuery(plan: LogicalPlan)(using context: Context): QueryResult =
    plan match
      case q: Relation =>
        val generatedSQL = GenSQL.generateSQLFromRelation(q)
        workEnv.info(s"Executing SQL:\n${generatedSQL.sql}")
        debug(s"Executing SQL:\n${generatedSQL.sql}")
        given monitor: QueryProgressMonitor = context.queryProgressMonitor
        TableRows.fromCrossPlatformResult(activeConnector.execute(generatedSQL.sql), rowLimit)
      case _ =>
        QueryResult.empty

  override protected def runStatements(sqls: List[String])(using context: Context): Unit = sqls
    .foreach { sql =>
      workEnv.info(s"Executing SQL:\n${sql}")
      debug(s"Executing SQL:\n${sql}")
      given monitor: QueryProgressMonitor = context.queryProgressMonitor
      activeConnector.execute(sql)
    }

  override protected def executeUseConnector(u: UseConnector)(using context: Context): QueryResult =
    switchConnector(u.connector.fullName.split("\\.").toList)

  override protected def executeUseSchema(u: UseSchema)(using context: Context): QueryResult =
    // Connector names of the active profile shadow schema names, so `use td` switches the
    // connector when `td` is one (same rule as the JVM QueryExecutor)
    val parts = u.schema.fullName.split("\\.").toList
    parts match
      case name :: _ if defaultProfile.connectors.exists(_.name == name) =>
        switchConnector(parts)
      case schema :: Nil =>
        context.global.defaultSchema = schema
        workEnv.info(s"Switched to schema: ${schema}")
        QueryResult.empty
      case catalogName :: schema :: Nil =>
        context.global.defaultSchema = schema
        workEnv.info(s"Switched to schema: ${schema}")
        QueryResult.empty
      case _ =>
        throw StatusCode
          .SYNTAX_ERROR
          .newException(
            s"Invalid schema name: ${u
                .schema
                .fullName}. Expected format: <schema_name> or <catalog_name>.<schema_name>"
          )

  private def switchConnector(parts: List[String])(using context: Context): QueryResult =
    val connectorName = parts.head
    val config        = defaultProfile
      .connectors
      .find(_.name == connectorName)
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Connector '${connectorName}' is not defined in profile '${defaultProfile.name}' " +
              s"(available: ${defaultProfile.connectors.map(_.name).mkString(", ")})"
          )
      )
    val (catalogName, schemaName) =
      parts.tail match
        case Nil =>
          (config.catalog, config.schema)
        case schema :: Nil =>
          (config.catalog, Some(schema))
        case catalog :: schema :: Nil =>
          (Some(catalog), Some(schema))
        case _ =>
          throw StatusCode
            .SYNTAX_ERROR
            .newException(
              s"Invalid connector reference: ${parts.mkString(".")}. " +
                "Expected format: <connector>, <connector>.<schema>, or <connector>.<catalog>.<schema>"
            )
    // Resolve the connector BEFORE committing state, so an unsupported type or connection
    // failure leaves the previous engine fully active
    connectorProvider.getConnector(config)
    activeConfig = config
    // Switch the SQL dialect along with the engine: SQL is generated at execution time from
    // context.dbType (= defaultCatalog.dbType), so replacing the default catalog makes every
    // statement after this `use` compile to the new engine's dialect. The thin CLIs run
    // catalog-free, so an in-memory catalog carrying the dialect (and catalog/schema names for
    // qualification) is all the switched engine needs
    val newDBType = DBType.fromString(config.`type`)
    context.global.defaultCatalog = InMemoryCatalog(
      catalogName = catalogName.getOrElse(connectorName),
      functions = Nil,
      catalogDBType = newDBType
    )
    schemaName.foreach { schema =>
      context.global.defaultSchema = schema
    }
    workEnv.info(s"Switched to connector: ${connectorName} (dialect: ${newDBType})")
    QueryResult.empty

  end switchConnector

end PlanExecutor
