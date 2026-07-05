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
import wvlet.lang.catalog.Catalog
import wvlet.lang.model.RelationType
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONValue

/**
  * A named connection activated by a profile. A connector may implement any combination of
  * capabilities:
  *   - [[catalog]]: expose tables to the compiler's namespace registry (`from <name>.<table>`)
  *   - [[engine]]: execute SQL plans (DuckDB, Trino, Snowflake, ...)
  *   - [[tools]]: MCP-shaped callable methods (e.g., `slack.post_message`)
  *
  * See https://github.com/wvlet/wvlet/issues/1861 for the design.
  */
trait Connector extends AutoCloseable:
  /** Instance name given in the profile's connector config (referenced from queries) */
  def name: String

  /** Connector implementation key, e.g. "trino", "duckdb", "slack" */
  def connectorType: String

  /** Tables this connector exposes to the compiler's namespace registry */
  def catalog: Option[CatalogProvider] = None

  /** SQL-plan execution capability; only engines carry a DBType/SQL dialect */
  def engine: Option[SqlEngine] = None

  /** MCP-compatible callable methods provided by this connector */
  def tools: Seq[ToolSpec] = Nil

  /** Invoke one of the [[tools]] by name */
  def invoke(tool: String, args: JSONObject): ToolResult =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException(s"Connector '${name}' (${connectorType}) has no tool '${tool}'")

end Connector

/**
  * Table-exposure capability of a [[Connector]]. Phase-1 skeleton: `scan(table, filters)` for
  * materializing source-connector tables into an engine lands with the first non-SQL connector
  */
trait CatalogProvider:
  def listTables: Seq[Catalog.TableName]
  def schemaOf(table: String): Option[RelationType]

/**
  * MCP-compatible tool description: `inputSchema` is a JSON Schema object for the tool arguments
  */
case class ToolSpec(name: String, description: String, inputSchema: JSONObject)

/** Result of a [[Connector.invoke]] call */
case class ToolResult(content: JSONValue, isError: Boolean = false)
