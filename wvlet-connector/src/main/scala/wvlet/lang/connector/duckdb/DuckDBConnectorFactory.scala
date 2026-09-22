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
package wvlet.lang.connector.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory

object DuckDBConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "duckdb"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector =
    def property[A](key: String, default: A)(parse: String => A): A = config
      .properties
      .get(key)
      .map { v =>
        try
          parse(v.toString)
        catch
          case e: IllegalArgumentException =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Invalid value '${v}' for property ${key} of connector ${config.name}",
                e
              )
      }
      .getOrElse(default)
    def booleanProperty(key: String): Boolean    = property(key, false)(_.toBoolean)
    def scaleFactorProperty(key: String): Double =
      property(key, DuckDBConnector.defaultScaleFactor)(_.toDouble)
    DuckDBConnector(
      workEnv,
      prepareTPCH = booleanProperty("prepareTPCH"),
      prepareTPCDS = booleanProperty("prepareTPCDS"),
      tpchScaleFactor = scaleFactorProperty("tpchScaleFactor"),
      tpcdsScaleFactor = scaleFactorProperty("tpcdsScaleFactor")
    ).withName(config.name)

end DuckDBConnectorFactory

object GenericConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "generic"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector = GenericConnector(
    workEnv
  ).withName(config.name)
