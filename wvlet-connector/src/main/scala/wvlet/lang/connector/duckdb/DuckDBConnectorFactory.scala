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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory

object DuckDBConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "duckdb"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector = DuckDBConnector(
    workEnv,
    prepareTPCH = config.properties.getOrElse("prepareTPCH", "false").toString.toBoolean,
    prepareTPCDS = config.properties.getOrElse("prepareTPCDS", "false").toString.toBoolean
  ).withName(config.name)

object GenericConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "generic"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector = GenericConnector(
    workEnv
  ).withName(config.name)
