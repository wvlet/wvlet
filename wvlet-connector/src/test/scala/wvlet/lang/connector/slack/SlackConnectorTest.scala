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
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.test.UniTest
import wvlet.uni.test.defined

/** An in-memory Slack workspace for tests */
class FakeSlackApiClient extends SlackApiClient:
  val posted = scala.collection.mutable.ListBuffer.empty[(String, String)]

  override def listChannels(): List[SlackChannel] = List(
    SlackChannel("C1", "general", isPrivate = false, numMembers = 12, topic = "Announcements"),
    SlackChannel("C2", "dev", isPrivate = false, numMembers = 5, topic = "")
  )

  override def listUsers(): List[SlackUser] = List(
    SlackUser("U1", "alice", "Alice A", isBot = false),
    SlackUser("U2", "wvlet-bot", "Wvlet Bot", isBot = true)
  )

  override def channelHistory(channelId: String, limit: Int): List[SlackMessage] =
    channelId match
      case "C1" =>
        List(
          SlackMessage("C1", "U1", "hello wvlet", "1751692800.000100", None),
          SlackMessage(
            "C1",
            "U2",
            "flow daily completed",
            "1751692900.000200",
            Some("1751692800.000100")
          )
        )
      case _ =>
        List(SlackMessage("C2", "U1", "deploy done", "1751693000.000300", None))

  override def postMessage(channel: String, text: String): Unit = posted += (channel -> text)

end FakeSlackApiClient

class SlackConnectorTest extends UniTest:

  private def newConnector(client: FakeSlackApiClient = FakeSlackApiClient()): SlackConnector =
    SlackConnector("slack", client)

  test("should list the workspace tables") {
    val catalog = newConnector().catalog.get
    catalog.listTables.map(_.name).toSet shouldBe Set("channels", "users", "messages")
  }

  test("should describe table schemas") {
    val catalog = newConnector().catalog.get
    val schema  = catalog.schemaOf("channels")
    schema shouldBe defined
    schema.get.fields.map(_.name.name) shouldBe
      List("id", "name", "is_private", "num_members", "topic")
    catalog.schemaOf("no_such_table") shouldBe None
  }

  test("should scan channels as JSON rows") {
    val rows = newConnector().catalog.get.scan("channels")
    rows.size shouldBe 2
    rows.head shouldContain "\"name\":\"general\""
    rows.head shouldContain "\"num_members\":12"
  }

  test("should scan messages across channels") {
    val rows = newConnector().catalog.get.scan("messages")
    rows.size shouldBe 3
    rows.head shouldContain "\"channel\":\"general\""
    rows.head shouldContain "hello wvlet"
  }

  test("should reject scans of unknown tables") {
    val exception = intercept[WvletLangException] {
      newConnector().catalog.get.scan("nope")
    }
    exception.statusCode shouldBe StatusCode.TABLE_NOT_FOUND
  }

  test("should expose post_message as an MCP-shaped tool and invoke it") {
    val client    = FakeSlackApiClient()
    val connector = newConnector(client)
    connector.tools.map(_.name) shouldBe Seq("post_message")
    connector.tools.head.inputSchema.toJSON shouldContain "\"channel\""

    connector.invoke(
      "post_message",
      JSONObject(Seq("channel" -> JSONString("#general"), "text" -> JSONString("hi")))
    )
    client.posted.toList shouldBe List("#general" -> "hi")
  }

  test("should fail with a clear error for unknown tools and missing arguments") {
    val connector = newConnector()
    intercept[WvletLangException] {
      connector.invoke("no_such_tool", JSONObject(Seq.empty))
    }.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
    intercept[WvletLangException] {
      connector.invoke("post_message", JSONObject(Seq("channel" -> JSONString("#g"))))
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("should require a token in the factory") {
    val exception = intercept[WvletLangException] {
      SlackConnectorFactory.create(ConnectorConfig(name = "slack", `type` = "slack"), WorkEnv())
    }
    exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    exception.message shouldContain "token"
  }

end SlackConnectorTest
