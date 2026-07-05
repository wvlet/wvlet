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
package wvlet.lang.connector.snowflake

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory

object SnowflakeConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "snowflake"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector = SnowflakeConnector(
    SnowflakeConfig(
      account = config
        .host
        .getOrElse(
          throw IllegalArgumentException("Snowflake connector requires 'host' (account identifier)")
        ),
      database = config
        .catalog
        .getOrElse(
          throw IllegalArgumentException("Snowflake connector requires 'catalog' (database name)")
        ),
      schema = config.schema.getOrElse("PUBLIC"),
      warehouse = config.properties.get("warehouse").map(_.toString),
      role = config.properties.get("role").map(_.toString),
      user = config.user,
      password = config.password
    ),
    workEnv
  ).withName(config.name)
