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

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.compiler.analyzer.duckdb.DuckDBSqlConnector
import wvlet.lang.compiler.analyzer.trino.TrinoConfig
import wvlet.lang.compiler.analyzer.trino.TrinoSqlConnector
import wvlet.lang.compiler.connector.SqlConnector
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Cross-platform [[SqlConnector]] registry: resolves a [[ConnectorConfig]] (from a profile or
  * assembled from CLI flags) to a connector for the engines available on every platform —
  * session-backed DuckDB and Trino over REST. Connectors are cached by config value equality (same
  * rule as the JVM `ConnectorProvider`) and closed together via [[close]].
  *
  * JVM-only engine types (snowflake, generic, slack, …) are NOT handled here; the JVM runner's
  * `ConnectorProvider` remains the full-featured registry. Requesting such a type raises a clear
  * error pointing at the JVM CLI.
  */
class SqlConnectorProvider(profile: Profile = Profile.defaultDuckDBProfile)
    extends AutoCloseable
    with LogSupport:

  private val cache = mutable.LinkedHashMap.empty[ConnectorConfig, SqlConnector]

  /** The connector for the profile's default engine. */
  def defaultConnector: SqlConnector = getConnector(profile.defaultEngine)

  /** Return a cached connector for `config`, creating (and caching) it on first use. */
  def getConnector(config: ConnectorConfig): SqlConnector = cache.getOrElseUpdate(
    config,
    createConnector(config)
  )

  private def createConnector(config: ConnectorConfig): SqlConnector =
    config.`type`.toLowerCase match
      case "duckdb" =>
        if !DuckDB.canExecute then
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(
              "DuckDB execution is not available on this platform. " +
                "Ensure libduckdb is installed and discoverable, or set WVLET_LIBDUCKDB."
            )
        // A persistent session so temp tables and multi-statement scripts survive across
        // execute calls. The optional `path` property opens a file-backed database.
        DuckDBSqlConnector.withNewSession(config.properties.get("path").map(_.toString))
      case "trino" =>
        val host = config
          .host
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Trino connector '${config
                    .name}' requires a host — pass --host or set 'host' on the profile"
              )
          )
        val useHttps = config.useHttps.getOrElse(false)
        TrinoSqlConnector(
          TrinoConfig(
            host = host,
            port = config
              .port
              .getOrElse(
                if useHttps then
                  443
                else
                  8080
              ),
            user = config.user.getOrElse("wvlet"),
            catalog = config.catalog,
            schema = config.schema,
            useHttps = useHttps,
            // Basic auth from the standard password field; bearer tokens (JWT / OAuth2) from
            // the `token` property. Profile values support ${ENV} interpolation, so secrets
            // stay out of profiles.json itself
            password = config.password,
            token = config.properties.get("token").map(_.toString)
          )
        )
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"Connector type '${other}' is not supported by the cross-platform runner " +
              s"(supported: duckdb, trino). Use the JVM wvlet CLI for '${other}'."
          )

  override def close(): Unit =
    cache
      .values
      .foreach { connector =>
        try
          connector.close()
        catch
          case NonFatal(e) =>
            warn(s"Failed to close connector: ${e.getMessage}")
      }
    cache.clear()

end SqlConnectorProvider
