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

import io.trino.jdbc.QueryStats as TrinoJdbcStats
import io.trino.jdbc.TrinoConnection
import io.trino.jdbc.TrinoDriver
import io.trino.jdbc.TrinoStatement
import wvlet.lang.catalog.SQLFunction
import wvlet.uni.control.Control
import wvlet.uni.util.Count
import wvlet.uni.util.ElapsedTime
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.analyzer.trino.TrinoConfig as TrinoXPConfig
import wvlet.lang.compiler.analyzer.trino.TrinoSqlConnector
import wvlet.lang.compiler.connector.QueryHandle
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.connector.QueryStats
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.DataType
import wvlet.lang.runner.connector.*
import wvlet.uni.log.LogSupport

import java.sql.Connection
import java.sql.Statement
import java.util.Properties
import java.util.function.Consumer

case class TrinoConfig(
    catalog: String,
    schema: String,
    hostAndPort: String,
    useSSL: Boolean = true,
    user: Option[String] = None,
    password: Option[String] = None
)

class TrinoConnector(val config: TrinoConfig, workEnv: WorkEnv)
    extends DBConnector(DBType.Trino, workEnv)
    with LogSupport:
  private lazy val driver = new TrinoDriver()

  /**
    * HTTP-backed [[SqlConnector]] view of this Trino instance, exposed as a separate accessor
    * because [[DBConnector]] and [[SqlConnector]] both declare `execute(sql)` with different return
    * types (Boolean vs QueryResult) and can't coexist on one class. Built lazily so the JDBC code
    * path (still the default for `QueryExecutor`'s `runQuery` / `executeUpdate`) doesn't pay any
    * HTTP setup cost when the caller never asks for it.
    *
    * Re-uses the cross-platform `TrinoSqlConnector` from `wvlet-lang` so the runner doesn't
    * reimplement the protocol. Once `QueryExecutor` migrates to this path (PR-B), we can drop the
    * JDBC half of `TrinoConnector` and stop dragging in `trino-jdbc`.
    */
  lazy val asSqlConnector: SqlConnector = TrinoSqlConnector(toCrossPlatformConfig)

  /**
    * Bridge the runner's JDBC-shaped [[TrinoConfig]] to the cross-platform variant in `wvlet-lang`.
    * `hostAndPort` collapses host + port into a single string for the JDBC URL; the cross-platform
    * shape wants them separate, with the port autoderived from the scheme when missing. `password`
    * is currently dropped on the HTTP path — auth on the cross-platform connector is `X-Trino-User`
    * only until the auth story (Basic/JWT) lands.
    */
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

  private[connector] override lazy val newConnection: DBConnection =
    val jdbcUrl =
      s"jdbc:trino://${config.hostAndPort}/${config.catalog}/${config.schema}${
          if config.useSSL then
            "?SSL=true"
          else
            ""
        }"
    trace(s"Connecting to Trino: ${jdbcUrl}")
    val properties = new Properties()
    config.user.foreach(x => properties.put("user", x))
    config.password.foreach(x => properties.put("password", x))
    DBConnection(driver.connect(jdbcUrl, properties).asInstanceOf[TrinoConnection])

  override def close(): Unit = Control.closeResources(newConnection, driver)

  override protected def withConnection[U](body: DBConnection => U): U =
    val conn = newConnection
    // Do not close the connection for reusing the connection
    body(conn)

  def withConfig(newConfig: TrinoConfig): TrinoConnector = new TrinoConnector(newConfig, workEnv)

  override protected def withStatement[U](body: Statement => U)(using
      queryProgressMonitor: QueryProgressMonitor = QueryProgressMonitor.noOp
  ): U = withConnection: conn =>
    Control.withResource(conn.createStatement().asInstanceOf[TrinoStatement]): stmt =>
      try
        stmt.setProgressMonitor(
          new Consumer[TrinoJdbcStats]:
            override def accept(stats: TrinoJdbcStats): Unit = queryProgressMonitor.reportProgress(
              QueryStats(
                state = QueryState.fromTrino(stats.getState),
                rowsProcessed = Some(stats.getProcessedRows),
                bytesProcessed = Some(stats.getProcessedBytes),
                elapsedMs = Some(stats.getElapsedTimeMillis),
                splitsCompleted = Some(stats.getCompletedSplits),
                splitsTotal = Some(stats.getTotalSplits)
              )
            )
        )
        body(stmt)
      finally
        stmt.clearProgressMonitor()

  override def listFunctions(catalog: String): List[SQLFunction] =
    val functionList = List.newBuilder[SQLFunction]
    runQuery("show functions") { rs =>
      while rs.next() do
        val returnType    = DataType.parse(rs.getString("Return Type"))
        val argumentTypes = rs.getString("Argument Types").split(", ").map(DataType.parse).toList
        val prop          = Map.newBuilder[String, Any]
        Option(rs.getString("description")).foreach(x => prop += "description" -> x)
        functionList +=
          SQLFunction(
            name = rs.getString("Function"),
            functionType = SQLFunction
              .FunctionType
              .valueOf(rs.getString("Function Type").toUpperCase),
            returnType = returnType,
            args = argumentTypes,
            properties = prop.result()
          )
    }
    functionList.result()

end TrinoConnector
