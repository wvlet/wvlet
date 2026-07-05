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
package wvlet.lang.connector.trino

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory

object TrinoConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "trino"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector =
    val useSSL = config.properties.get("useSSL").flatMap(parseBoolean).getOrElse(true)
    TrinoConnector(
      TrinoConfig(
        catalog = config.catalog.getOrElse("default"),
        schema = config.schema.getOrElse("default"),
        hostAndPort = config.host.getOrElse("localhost"),
        useSSL = useSSL,
        user = config.user,
        password = config.password
      ),
      workEnv
    ).withName(config.name)

  private def parseBoolean(v: Any): Option[Boolean] =
    v match
      case b: java.lang.Boolean =>
        Some(b.booleanValue())
      case b: Boolean =>
        Some(b)
      case n: java.lang.Integer =>
        Some(n.intValue() != 0)
      case n: Int =>
        Some(n != 0)
      case s: String =>
        s.trim.toLowerCase match
          case "true" | "1" | "yes" | "y" | "on" =>
            Some(true)
          case "false" | "0" | "no" | "n" | "off" =>
            Some(false)
          case _ =>
            None
      case _ =>
        None

end TrinoConnectorFactory
