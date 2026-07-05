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
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory
import wvlet.lang.connector.DBConnector
import wvlet.lang.connector.duckdb.DuckDBConnectorFactory
import wvlet.lang.connector.duckdb.GenericConnectorFactory
import wvlet.lang.connector.slack.SlackConnectorFactory
import wvlet.lang.connector.snowflake.SnowflakeConnectorFactory
import wvlet.lang.connector.trino.TrinoConnectorFactory
import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
  * Resolves [[Connector]] instances from a profile's connector configs through an explicit
  * [[ConnectorFactory]] registry, caching one live connector per distinct [[ConnectorConfig]]
  */
class ConnectorProvider(
    workEnv: WorkEnv,
    factories: Seq[ConnectorFactory] = ConnectorProvider.defaultFactories
) extends LogSupport
    with AutoCloseable:

  private val factoryMap: Map[String, ConnectorFactory] =
    factories.map(f => f.connectorType -> f).toMap

  // Keyed by ConnectorConfig value equality: precise (unlike whole-Profile keys, which fork a
  // connector on any unrelated profile edit) and sound (unlike name keys — differently configured
  // connectors may share a name across ad-hoc profiles).
  private val connectorCache = ConcurrentHashMap[ConnectorConfig, Connector]()

  override def close(): Unit = connectorCache.values().asScala.foreach(_.close())

  // computeIfAbsent (not the Scala wrapper's getOrElseUpdate) so a race on first use cannot
  // create two live connectors and silently leak the losing one
  def getConnector(config: ConnectorConfig): Connector = connectorCache.computeIfAbsent(
    config,
    c => createConnector(c)
  )

  private def createConnector(config: ConnectorConfig): Connector =
    factoryMap.get(config.`type`.toLowerCase) match
      case Some(factory) =>
        factory.create(config, workEnv)
      case None =>
        // Preserve the compile-only fallback for engines without a runtime connector (-t hive etc.)
        warn(
          s"Connector for type '${config
              .`type`}' is not available. Using the generic DuckDB fallback"
        )
        factoryMap
          .get("generic")
          .map(_.create(config.copy(`type` = "generic"), workEnv))
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"No connector factory for type '${config.`type`}' (registered: ${factoryMap
                    .keys
                    .mkString(", ")})"
              )
          )

  /** Resolve the profile's default engine as a SQL engine connector */
  def getConnector(profile: Profile): DBConnector = getDBConnector(profile.defaultEngine)

  /** Resolve a connector config as a SQL engine connector */
  def getDBConnector(config: ConnectorConfig): DBConnector =
    getConnector(config) match
      case db: DBConnector =>
        db
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Connector '${other.name}' (${other.connectorType}) is not a SQL engine")

  def getConnector(dbType: DBType, profileName: Option[String]): DBConnector =
    val profile = profileName
      .flatMap(Profile.getProfile)
      .getOrElse(Profile.defaultProfileFor(dbType))
    getConnector(profile)

end ConnectorProvider

object ConnectorProvider:
  /** All built-in engine connectors */
  def defaultFactories: Seq[ConnectorFactory] = Seq(
    DuckDBConnectorFactory,
    GenericConnectorFactory,
    TrinoConnectorFactory,
    SnowflakeConnectorFactory,
    SlackConnectorFactory
  )
