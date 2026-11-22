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
package wvlet.lang.runner.engine

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory

/**
  * The in-process wvlet engine as a profile connector (`"type": "wvlet"`). It executes no SQL, so
  * it is not a [[wvlet.lang.connector.DBConnector]]: the [[wvlet.lang.runner.QueryExecutor]] runs
  * queries of this connector on [[session]] directly
  */
class WvletEngineConnector(config: ConnectorConfig, workEnv: WorkEnv) extends Connector:
  val session: WvletEngineSession = WvletEngineSession(config, workEnv.path)

  override def name: String          = config.name
  override def connectorType: String = WvletEngineSession.connectorType
  override def close(): Unit         = ()

object WvletEngineConnectorFactory extends ConnectorFactory:
  override def connectorType: String = WvletEngineSession.connectorType

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector =
    if !WvletEngineSession.isInProcess(config) then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Connector '${config.name}' points to a remote wvlet server (host: ${config
              .host
              .getOrElse(
                ""
              )}), which only the cross-platform CLI (wvc, npm @wvlet/cli) supports. " +
            "Remove the host to use the in-process wvlet engine"
        )
    WvletEngineConnector(config, workEnv)
