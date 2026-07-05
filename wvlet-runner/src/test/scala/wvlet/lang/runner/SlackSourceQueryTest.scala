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
package wvlet.lang.runner

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.LazyCatalog
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.Connector
import wvlet.lang.connector.ConnectorFactory
import wvlet.lang.connector.slack.SlackChannel
import wvlet.lang.connector.slack.SlackConnector
import wvlet.lang.connector.slack.SlackMessage
import wvlet.lang.connector.slack.SlackApiClient
import wvlet.lang.connector.slack.SlackUser
import wvlet.lang.runner.connector.ConnectorCatalogs
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

/**
  * End-to-end: a Slack source connector queried from a DuckDB engine (#1861 Phase 3). Slack tables
  * are staged into the active engine transparently; flows deliver stage outputs through the
  * connector's post_message tool
  */
class SlackSourceQueryTest extends UniTest:

  private class RecordingSlackClient extends SlackApiClient:
    val posted = scala.collection.mutable.ListBuffer.empty[(String, String)]
    override def listChannels(): List[SlackChannel] = List(
      SlackChannel("C1", "general", isPrivate = false, numMembers = 12, topic = ""),
      SlackChannel("C2", "dev", isPrivate = false, numMembers = 5, topic = "")
    )

    override def listUsers(): List[SlackUser] = List(
      SlackUser("U1", "alice", "Alice A", isBot = false)
    )

    override def channelHistory(channelId: String, limit: Int): List[SlackMessage] = List(
      SlackMessage(channelId, "U1", s"message in ${channelId}", "1751692800.000100", None)
    )

    override def postMessage(channel: String, text: String): Unit = posted += (channel -> text)

  private val fakeSlack = RecordingSlackClient()

  // A factory returning the fake-backed connector, replacing the HTTP-based default
  private object FakeSlackFactory extends ConnectorFactory:
    override def connectorType: String                                        = "slack"
    override def isEngine: Boolean                                            = false
    override def create(config: ConnectorConfig, workEnv: WorkEnv): Connector = SlackConnector(
      config.name,
      fakeSlack
    )

  private val workEnv = WorkEnv()
  private val duckdb  = ConnectorConfig(
    name = "duckdb",
    `type` = "duckdb",
    default = true,
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val slack   = ConnectorConfig(name = "slack", `type` = "slack")
  private val profile = Profile(name = "dev", connectors = Seq(duckdb, slack))

  private val provider = ConnectorProvider(
    workEnv,
    factories =
      ConnectorProvider.defaultFactories.filterNot(_.connectorType == "slack") :+ FakeSlackFactory
  )

  private val executor = QueryExecutor(provider, profile, workEnv)

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List("."), workEnv = workEnv, catalog = Some("memory"))
  )

  profile
    .connectors
    .foreach { c =>
      val schema      = c.schema.getOrElse("main")
      val catalogName = c.catalog.getOrElse(c.name)
      compiler.addConnectorCatalog(
        c.name,
        LazyCatalog(
          catalogName,
          c.dbType,
          () => ConnectorCatalogs.catalogOf(provider.getConnector(c), c, catalogName, schema)
        ),
        schema
      )
    }

  compiler.setDefaultCatalog(provider.getDBConnector(duckdb).getCatalog("memory", "main"))
  compiler.setDefaultSchema("main")

  override def afterAll: Unit =
    executor.close()
    provider.close()

  private def run(statement: String): QueryResult =
    val unit   = CompilationUnit.fromWvletString(statement)
    val result = compiler.compileSingleUnit(unit)
    executor.executeSingle(unit, result.context)

  test("should query a Slack table from the DuckDB engine via automatic staging") {
    val result = run("from slack.channels | where is_private = false | select name")
    val tsv    = result.toTSV
    tsv shouldContain "general"
    tsv shouldContain "dev"
  }

  test("should aggregate over staged Slack messages") {
    val result = run("from slack.messages | select count(*) as cnt")
    result.toTSV shouldContain "2"
  }

  test("should deliver a flow stage output through the post_message tool") {
    val result = run("""flow notify_flow = {
        |  stage summary = from slack.channels | select count(*) as channels
        |  stage send = from summary | activate('slack', tool: 'post_message', channel: '#reports')
        |}
        |
        |run flow notify_flow""".stripMargin)
    result.isSuccess shouldBe true
    fakeSlack.posted.size shouldBe 1
    fakeSlack.posted.head._1 shouldBe "#reports"
    fakeSlack.posted.head._2 shouldContain "channels"
  }

  test("should invoke a connector tool ad hoc with a call statement") {
    fakeSlack.posted.clear()
    val result = run("call slack.post_message(channel: '#general', text: 'hello from wvlet')")
    result.isSuccess shouldBe true
    fakeSlack.posted.toList shouldBe List("#general" -> "hello from wvlet")
    val tsv = result.toTSV
    tsv shouldContain "post_message"
    tsv shouldContain "ok"
  }

  test("should pipe a call statement result through query operators") {
    val result = run("""call slack.post_message(channel: '#general', text: 'piped')
        |select connector, tool, status
        |""".stripMargin)
    result.toTSV shouldContain "slack\tpost_message\tok"
  }

  test("should verify a call statement result with a test statement") {
    val result = run("""call slack.post_message(channel: '#general', text: 'tested')
        |test _.columns should contain 'status'
        |""".stripMargin)
    result.isSuccess shouldBe true
  }

  test("should report an unknown tool in a call statement") {
    val e = intercept[Exception] {
      run("call slack.no_such_tool(text: 'x')")
    }
    e.getMessage shouldContain "no tool 'no_such_tool'"
    e.getMessage shouldContain "post_message"
  }

  test("should report an unknown connector in a call statement") {
    val e = intercept[Exception] {
      run("call nowhere.post_message(text: 'x')")
    }
    e.getMessage shouldContain "'nowhere' is not found"
  }

  test("should reject positional arguments in a call statement") {
    val e = intercept[Exception] {
      run("call slack.post_message('#general')")
    }
    e.getMessage shouldContain "must be named"
  }

end SlackSourceQueryTest
