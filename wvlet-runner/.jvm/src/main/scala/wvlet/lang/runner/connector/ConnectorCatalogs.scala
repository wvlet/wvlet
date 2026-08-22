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
import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorCatalogAdapter
import wvlet.lang.connector.DBConnector

object ConnectorCatalogs:

  /**
    * The compiler-facing [[Catalog]] of a connector: SQL engines expose their database catalog;
    * source connectors expose their [[wvlet.lang.connector.CatalogProvider]] tables through a
    * read-only adapter
    */
  def catalogOf(
      connector: Connector,
      config: ConnectorConfig,
      catalogName: String,
      schema: String
  ): Catalog =
    connector match
      case db: DBConnector =>
        db.getCatalog(catalogName, schema)
      case other =>
        other
          .catalog
          .map(provider => ConnectorCatalogAdapter(catalogName, provider))
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Connector '${config.name}' (${other.connectorType}) exposes no tables"
              )
          )
