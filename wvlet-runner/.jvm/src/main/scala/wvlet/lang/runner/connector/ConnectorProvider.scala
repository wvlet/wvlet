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
import wvlet.lang.connector.ConnectorFactoryRegistry
import wvlet.lang.connector.DBConnector
import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
  * Resolves [[Connector]] instances from a profile's connector configs through a
  * [[ConnectorFactory]] registry, caching one live connector per distinct [[ConnectorConfig]]. By
  * default the registry holds the factories found on the classpath ([[ConnectorFactoryRegistry]]),
  * so optional connector modules (DuckDB, Snowflake) are used when installed
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
  /** Whether configs of this type create SQL engines, decided without instantiating one */
  def isEngineType(connectorType: String): Boolean = factoryMap
    .get(connectorType.toLowerCase)
    .forall(_.isEngine)

  def getConnector(config: ConnectorConfig): Connector = connectorCache.computeIfAbsent(
    config,
    c => createConnector(c)
  )

  /**
    * Whether this provider can create connectors of the type, directly or (for a type that is not
    * an optional module) through the generic DuckDB fallback
    */
  def isAvailable(connectorType: String): Boolean =
    val tpe = connectorType.toLowerCase
    factoryMap.contains(tpe) ||
    (!ConnectorFactoryRegistry.isOptional(tpe) && factoryMap.contains("generic"))

  private def createConnector(config: ConnectorConfig): Connector =
    val tpe = config.`type`.toLowerCase
    factoryMap.get(tpe) match
      case Some(factory) =>
        factory.create(config, workEnv)
      case None if ConnectorFactoryRegistry.isOptional(tpe) =>
        // A known connector whose module this distribution leaves out: tell how to install it
        // rather than silently running on another engine
        throw ConnectorFactoryRegistry.notInstalled(tpe, config.name)
      case None =>
        factoryMap.get("generic") match
          case Some(generic) =>
            // Preserve the compile-only fallback for engines without a runtime connector
            // (-t hive etc.)
            warn(
              s"Connector for type '${config
                  .`type`}' is not available. Using the generic DuckDB fallback"
            )
            generic.create(config.copy(`type` = "generic"), workEnv)
          case None =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"No connector for type '${config.`type`}' (for '${config
                    .name}'). Available: ${factoryMap.keys.toSeq.sorted.mkString(", ")}"
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
  /** The connector factories registered on the classpath through `META-INF/services` */
  def defaultFactories: Seq[ConnectorFactory] = ConnectorFactoryRegistry.factories
