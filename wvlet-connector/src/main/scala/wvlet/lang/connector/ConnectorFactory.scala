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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv

/**
  * Creates a [[Connector]] from a profile's [[ConnectorConfig]]. Factories are registered
  * explicitly (no classpath scanning): each connector implementation exposes a factory object, and
  * the runtime assembles the registry from a literal list.
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
