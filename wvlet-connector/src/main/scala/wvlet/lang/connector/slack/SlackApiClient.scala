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
import wvlet.uni.http.Http
import wvlet.uni.http.HttpMethod
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.log.LogSupport

case class SlackChannel(
    id: String,
    name: String,
    isPrivate: Boolean,
    numMembers: Long,
    topic: String
)

case class SlackUser(id: String, name: String, realName: String, isBot: Boolean)

case class SlackMessage(
    channel: String,
    user: String,
    text: String,
    ts: String,
    threadTs: Option[String]
)

/**
  * The subset of the Slack Web API the connector consumes. Injectable so tests can run against an
  * in-memory workspace instead of HTTP
  */
trait SlackApiClient:
  def listChannels(): List[SlackChannel]
  def listUsers(): List[SlackUser]

  /** Latest messages of a channel, newest first, up to `limit` */
  def channelHistory(channelId: String, limit: Int): List[SlackMessage]

  def postMessage(channel: String, text: String): Unit

/**
  * Slack Web API client over uni's HTTP client. Handles cursor-based pagination for the list
  * endpoints and surfaces Slack's `ok: false` responses as errors
  */
class HttpSlackApiClient(token: String, baseUrl: String = "https://slack.com/api")
    extends SlackApiClient
    with LogSupport:

  private lazy val client: HttpSyncClient = Http.client.withBaseUri(baseUrl).newSyncClient

  private def call(method: String, params: Map[String, String]): JSONObject =
    val query = params
      .map { case (k, v) =>
        s"${k}=${java.net.URLEncoder.encode(v, "UTF-8")}"
      }
      .mkString("&")
    val path =
      if query.isEmpty then
        s"/${method}"
      else
        s"/${method}?${query}"
    val request = Request(method = HttpMethod.POST, uri = path).setHeader(
      "Authorization",
      s"Bearer ${token}"
    )
    val response = client.send(request)
    val body     = response.contentAsString.getOrElse("{}")
    JSON.parse(body) match
      case o: JSONObject if o.get("ok").contains(JSONBoolean(true)) =>
        o
      case o: JSONObject =>
        val error = o
          .get("error")
          .collect { case JSONString(s) =>
            s
          }
          .getOrElse("unknown error")
        throw StatusCode.INVALID_ARGUMENT.newException(s"Slack API ${method} failed: ${error}")
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Slack API ${method} returned unexpected response")

  end call

  // Iterate a cursor-paginated list endpoint, collecting `field` arrays until the cursor runs out
  private def paginate(
      method: String,
      params: Map[String, String],
      field: String
  ): List[JSONObject] =
    val results = List.newBuilder[JSONObject]
    var cursor  = ""
    var more    = true
    while more do
      val pageParams =
        if cursor.isEmpty then
          params
        else
          params + ("cursor" -> cursor)
      val page = call(method, pageParams)
      page.get(field) match
        case Some(JSONArray(items)) =>
          items.foreach {
            case o: JSONObject =>
              results += o
            case _ =>
          }
        case _ =>
      cursor = page
        .get("response_metadata")
        .collect { case m: JSONObject =>
          m
        }
        .flatMap(_.get("next_cursor"))
        .collect { case JSONString(s) =>
          s
        }
        .getOrElse("")
      more = cursor.nonEmpty
    results.result()

  end paginate

  private def str(o: JSONObject, key: String): String = o
    .get(key)
    .collect { case JSONString(s) =>
      s
    }
    .getOrElse("")

  private def bool(o: JSONObject, key: String): Boolean = o.get(key).contains(JSONBoolean(true))

  override def listChannels(): List[SlackChannel] = paginate(
    "conversations.list",
    Map("limit" -> "200", "types" -> "public_channel,private_channel"),
    "channels"
  ).map { c =>
    SlackChannel(
      id = str(c, "id"),
      name = str(c, "name"),
      isPrivate = bool(c, "is_private"),
      numMembers = c
        .get("num_members")
        .collect {
          case n: JSON.JSONLong =>
            n.v
          case n: JSON.JSONDouble =>
            n.v.toLong
        }
        .getOrElse(0L),
      topic = c
        .get("topic")
        .collect { case t: JSONObject =>
          str(t, "value")
        }
        .getOrElse("")
    )
  }

  override def listUsers(): List[SlackUser] = paginate(
    "users.list",
    Map("limit" -> "200"),
    "members"
  ).map { u =>
    SlackUser(
      id = str(u, "id"),
      name = str(u, "name"),
      realName = u
        .get("profile")
        .collect { case p: JSONObject =>
          str(p, "real_name")
        }
        .getOrElse(""),
      isBot = bool(u, "is_bot")
    )
  }

  override def channelHistory(channelId: String, limit: Int): List[SlackMessage] = paginate(
    "conversations.history",
    Map("channel" -> channelId, "limit" -> limit.min(200).toString),
    "messages"
  ).take(limit)
    .map { m =>
      SlackMessage(
        channel = channelId,
        user = str(m, "user"),
        text = str(m, "text"),
        ts = str(m, "ts"),
        threadTs = m
          .get("thread_ts")
          .collect { case JSONString(s) =>
            s
          }
      )
    }

  override def postMessage(channel: String, text: String): Unit = call(
    "chat.postMessage",
    Map("channel" -> channel, "text" -> text)
  )

end HttpSlackApiClient
