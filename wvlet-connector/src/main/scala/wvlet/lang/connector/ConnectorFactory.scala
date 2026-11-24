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
package wvlet.lang.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv

import java.util.ServiceLoader
import scala.jdk.CollectionConverters.*

/**
  * Creates a [[Connector]] from a profile's [[ConnectorConfig]]. Each connector module lists its
  * factories through a [[ConnectorFactoryProvider]] registered in `META-INF/services`, so optional
  * modules (DuckDB, Snowflake) join the registry just by being on the classpath.
  */
trait ConnectorFactory:
  /** The `type` value in the connector config this factory handles, e.g. "duckdb" */
  def connectorType: String

  /**
    * Whether the created connectors are SQL engines. Lets callers classify connectors from their
    * config type without instantiating them (which may open a connection)
    */
  def isEngine: Boolean = true

  def create(config: ConnectorConfig, workEnv: WorkEnv): Connector

/**
  * ServiceLoader entry point of a connector module. Factories are Scala objects, which
  * ServiceLoader cannot instantiate, so each module registers a provider class (named in
  * `META-INF/services/wvlet.lang.connector.ConnectorFactoryProvider`) that returns them.
  */
trait ConnectorFactoryProvider:
  def factories: Seq[ConnectorFactory]

/** Connector factories discovered on the classpath, and install hints for missing ones */
object ConnectorFactoryRegistry:

  /**
    * Connector types shipped as optional modules, with the module that provides them. Used to tell
    * users how to install a connector the current distribution leaves out.
    */
  val optionalConnectorModules: Map[String, String] = Map(
    "duckdb"    -> "wvlet-connector-duckdb",
    "generic"   -> "wvlet-connector-duckdb",
    "snowflake" -> "wvlet-connector-snowflake"
  )

  /** All factories registered on the classpath, discovered once */
  lazy val factories: Seq[ConnectorFactory] = discover()

  /** Whether a factory for the connector type is on the classpath */
  def isAvailable(connectorType: String): Boolean =
    val tpe = connectorType.toLowerCase
    factories.exists(_.connectorType == tpe)

  private def discover(): Seq[ConnectorFactory] =
    // Prefer the context class loader (sbt test layers, embedding apps), and fall back to the
    // loader of this class when it sees no provider
    val loaders =
      Seq(
        Option(Thread.currentThread().getContextClassLoader),
        Option(classOf[ConnectorFactoryProvider].getClassLoader)
      ).flatten.distinct
    val providers = loaders
      .iterator
      .map(cl => ServiceLoader.load(classOf[ConnectorFactoryProvider], cl).asScala.toList)
      .find(_.nonEmpty)
      .getOrElse(Nil)
    // Keep the first factory per type for a stable registry when a module appears twice
    providers
      .flatMap(_.factories)
      .foldLeft(List.empty[ConnectorFactory]) { (acc, f) =>
        if acc.exists(_.connectorType == f.connectorType) then
          acc
        else
          acc :+ f
      }

  /** Whether the connector type ships as an optional module */
  def isOptional(connectorType: String): Boolean = optionalConnectorModules.contains(
    connectorType.toLowerCase
  )

  /**
    * Error for an optional connector type (see [[optionalConnectorModules]]) whose module is not on
    * the classpath, telling how to install it
    */
  def notInstalled(connectorType: String, connectorName: String): WvletLangException =
    val tpe    = connectorType.toLowerCase
    val module = optionalConnectorModules.getOrElse(tpe, s"wvlet-connector-${tpe}")
    StatusCode
      .CONNECTOR_NOT_INSTALLED
      .newException(
        s"The ${tpe} connector (for '${connectorName}') is not installed in this wvlet " +
          s"distribution. Install the full CLI (wvlet-cli-full-<version>.tar.gz from " +
          s"https://github.com/wvlet/wvlet/releases), or copy the ${module} jar and its " +
          s"dependencies into the lib folder of the CLI"
      )

end ConnectorFactoryRegistry
