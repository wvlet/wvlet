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

import wvlet.lang.connector.slack.SlackConnectorFactory
import wvlet.lang.connector.trino.TrinoConnectorFactory

/** Registers the connectors bundled in wvlet-connector (Trino over REST, Slack) */
class BuiltinConnectorFactories extends ConnectorFactoryProvider:
  override def factories: Seq[ConnectorFactory] = Seq(TrinoConnectorFactory, SlackConnectorFactory)
