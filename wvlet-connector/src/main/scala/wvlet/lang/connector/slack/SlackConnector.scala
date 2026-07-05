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
package wvlet.lang.connector.slack

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.CatalogProvider
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory
import wvlet.lang.connector.ToolResult
import wvlet.lang.connector.ToolSpec
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.log.LogSupport

/**
  * Slack as a wvlet connector (#1861 Phase 3): exposes the workspace as tables (`channels`,
  * `users`, `messages`) through the [[CatalogProvider]] capability, and MCP-shaped tools
  * (`post_message`) through [[Connector.tools]]
  */
class SlackConnector(
    override val name: String,
    client: SlackApiClient,
    messageFetchLimit: Int = SlackConnector.DefaultMessageFetchLimit
) extends Connector
    with LogSupport:

  override def connectorType: String = "slack"

  override def close(): Unit = ()

  override def catalog: Option[CatalogProvider] = Some(SlackCatalogProvider)

  override val tools: Seq[ToolSpec] = Seq(
    ToolSpec(
      name = "post_message",
      description = "Post a message to a Slack channel",
      inputSchema = JSONObject(
        Seq(
          "type"       -> JSONString("object"),
          "properties" ->
            JSONObject(
              Seq(
                "channel" ->
                  JSONObject(
                    Seq(
                      "type"        -> JSONString("string"),
                      "description" -> JSONString("Channel name or ID to post to")
                    )
                  ),
                "text" ->
                  JSONObject(
                    Seq("type" -> JSONString("string"), "description" -> JSONString("Message text"))
                  )
              )
            ),
          "required" ->
            wvlet.uni.json.JSON.JSONArray(IndexedSeq(JSONString("channel"), JSONString("text")))
        )
      )
    )
  )

  override def invoke(tool: String, args: JSONObject): ToolResult =
    tool match
      case "post_message" =>
        def arg(key: String): String = args
          .get(key)
          .collect { case JSONString(s) =>
            s
          }
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"post_message requires a '${key}' argument")
          )
        client.postMessage(arg("channel"), arg("text"))
        ToolResult(JSONString("ok"))
      case other =>
        super.invoke(other, args)

  private object SlackCatalogProvider extends CatalogProvider:
    override def listTables: Seq[TableName] =
      SlackConnector.tableSchemas.keys.map(t => TableName(Some(name), Some("main"), t)).toSeq

    override def schemaOf(table: String): Option[RelationType] = SlackConnector
      .tableSchemas
      .get(table)

    override def scan(table: String): Seq[String] =
      table match
        case "channels" =>
          client
            .listChannels()
            .map { c =>
              row(
                "id"          -> JSONString(c.id),
                "name"        -> JSONString(c.name),
                "is_private"  -> JSONBoolean(c.isPrivate),
                "num_members" -> JSONLong(c.numMembers),
                "topic"       -> JSONString(c.topic)
              )
            }
        case "users" =>
          client
            .listUsers()
            .map { u =>
              row(
                "id"        -> JSONString(u.id),
                "name"      -> JSONString(u.name),
                "real_name" -> JSONString(u.realName),
                "is_bot"    -> JSONBoolean(u.isBot)
              )
            }
        case "messages" =>
          client
            .listChannels()
            .flatMap { channel =>
              client
                .channelHistory(channel.id, messageFetchLimit)
                .map { m =>
                  row(
                    "channel"   -> JSONString(channel.name),
                    "user"      -> JSONString(m.user),
                    "text"      -> JSONString(m.text),
                    "ts"        -> JSONString(m.ts),
                    "thread_ts" -> m.threadTs.map[JSONValue](JSONString(_)).getOrElse(JSONNull())
                  )
                }
            }
        case other =>
          throw StatusCode.TABLE_NOT_FOUND.newException(s"Slack connector has no table '${other}'")

    private def row(fields: (String, JSONValue)*): String = JSONObject(fields.toSeq).toJSON

  end SlackCatalogProvider

end SlackConnector

object SlackConnector:
  /** Messages fetched per channel when scanning the `messages` table */
  val DefaultMessageFetchLimit = 1000

  private def schema(table: String, fields: (String, DataType)*): SchemaType = SchemaType(
    None,
    Name.typeName(table),
    fields
      .map { case (n, t) =>
        NamedType(Name.termName(n), t)
      }
      .toList
  )

  private[slack] val tableSchemas: Map[String, SchemaType] = Map(
    "channels" ->
      schema(
        "channels",
        "id"          -> DataType.StringType,
        "name"        -> DataType.StringType,
        "is_private"  -> DataType.BooleanType,
        "num_members" -> DataType.LongType,
        "topic"       -> DataType.StringType
      ),
    "users" ->
      schema(
        "users",
        "id"        -> DataType.StringType,
        "name"      -> DataType.StringType,
        "real_name" -> DataType.StringType,
        "is_bot"    -> DataType.BooleanType
      ),
    "messages" ->
      schema(
        "messages",
        "channel"   -> DataType.StringType,
        "user"      -> DataType.StringType,
        "text"      -> DataType.StringType,
        "ts"        -> DataType.StringType,
        "thread_ts" -> DataType.StringType
      )
  )

end SlackConnector

object SlackConnectorFactory extends ConnectorFactory:
  override def connectorType: String = "slack"

  override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector =
    val token = config
      .properties
      .get("token")
      .map(_.toString)
      .orElse(config.password)
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Slack connector '${config
                .name}' requires a 'token' property (e.g. \"properties\": {\"token\": \"${'$'}{SLACK_TOKEN}\"})"
          )
      )
    val fetchLimit = config
      .properties
      .get("message_fetch_limit")
      .map(_.toString.toInt)
      .getOrElse(SlackConnector.DefaultMessageFetchLimit)
    SlackConnector(config.name, HttpSlackApiClient(token), fetchLimit)
