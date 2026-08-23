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
package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.ThreadManager
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest
import wvlet.uni.util.ULID

/**
  * Query lifecycle over [[QueryService]]: structured results with row limits, and best-effort
  * cancellation with its status guard (#1963 phase 4)
  */
class QueryServiceTest extends UniTest:

  private val workDir = new java.io.File("target/query-service-test")
  workDir.mkdirs()
  private val workEnv = WorkEnv(path = workDir.getPath)

  private val engine = ConnectorConfig(
    name = "duckdb",
    `type` = "duckdb",
    default = true,
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val profile = Profile(name = "test", connectors = Seq(engine))

  private val provider      = ConnectorProvider(workEnv)
  private val threadManager = ThreadManager()
  private val sessions      = ScriptRunnerSessions(
    workEnv,
    WvletScriptRunnerConfig(
      interactive = false,
      profile = profile,
      catalog = engine.catalog,
      schema = engine.schema
    ),
    provider,
    threadManager,
    ScriptRunnerSessions.DefaultIdleTimeout
  )

  private val service = QueryService(sessions)

  override def afterAll: Unit =
    service.close()
    sessions.close()
    threadManager.close()
    provider.close()

  private def awaitCompletion(queryId: ULID, timeoutMillis: Long = 30000): QueryInfo =
    val deadline = System.currentTimeMillis() + timeoutMillis
    var info     = service.fetchNext(QueryInfoRequest(queryId, pageToken = "0"))
    while !info.status.isFinished && System.currentTimeMillis() < deadline do
      Thread.sleep(50)
      info = service.fetchNext(QueryInfoRequest(queryId, pageToken = info.pageToken))
    info.status.isFinished shouldBe true
    info

  test("return structured rows along with the preview") {
    val response = service.enqueue(QueryRequest(query = "select 1 as x, 'hello' as s"))
    val info     = awaitCompletion(response.queryId)
    info.status shouldBe QueryStatus.FINISHED
    info.preview.isDefined shouldBe true
    val result = info.result.getOrElse(fail("result was not populated"))
    result.schema.map(_.name) shouldBe List("x", "s")
    result.rows.size shouldBe 1
    result.rows.head.map(v => Option(v).map(_.toString).orNull) shouldBe List("1", "hello")
  }

  test("bound structured rows by request.maxRows and report the total") {
    val response = service.enqueue(
      QueryRequest(
        query = "from [[1], [2], [3], [4], [5]] as t(a) select a order by a",
        maxRows = Some(2)
      )
    )
    val info   = awaitCompletion(response.queryId)
    val result = info.result.getOrElse(fail("result was not populated"))
    result.rows.size shouldBe 2
    result.actualTotalRows shouldBe Some(5)
  }

  test("report failures with error details and no result") {
    val response = service.enqueue(QueryRequest(query = "from table_that_does_not_exist_xyz"))
    val info     = awaitCompletion(response.queryId)
    info.status shouldBe QueryStatus.FAILED
    info.errors.nonEmpty shouldBe true
    info.result shouldBe None
  }

  test("reject cancellation of an unknown query") {
    intercept[WvletLangException] {
      service.cancel(ULID.newULID)
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("keep the final state when cancelling an already-finished query") {
    val response = service.enqueue(QueryRequest(query = "select 1"))
    awaitCompletion(response.queryId)
    val info = service.cancel(response.queryId)
    info.status shouldBe QueryStatus.FINISHED
  }

  test("cancellation is terminal — a racing completion never overwrites it") {
    // A fresh session id forces runner creation + first compilation, so the query is still in
    // flight when cancel lands; if it happens to finish first, cancel reports FINISHED instead.
    // Either way the state cancel returned must be the state that sticks.
    val response = service.enqueue(
      QueryRequest(query = "select 42", sessionId = Some(s"cancel-${ULID.newULIDString}"))
    )
    val onCancel = service.cancel(response.queryId)
    Thread.sleep(2000) // let the worker thread run to completion
    val finalInfo = service.fetchNext(QueryInfoRequest(response.queryId, pageToken = "2"))
    finalInfo.status shouldBe onCancel.status
    if onCancel.status == QueryStatus.CANCELED then
      finalInfo.result shouldBe None
  }

end QueryServiceTest
