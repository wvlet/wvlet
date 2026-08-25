package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.http.Http
import wvlet.uni.http.HttpClientConfig
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.http.Response
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.test.UniTest

/**
  * Protocol-level tests for [[TrinoQueryHandle]]'s streaming `batches()` iterator, driven by a fake
  * `HttpSyncClient` serving canned Trino client-protocol pages — no server needed, so the paging
  * behavior is verified on JVM, JS, and Native.
  */
class TrinoQueryHandleTest extends UniTest:

  private val trinoConfig = TrinoConfig(host = "localhost", user = "test")

  /** Serves canned response bodies by request URI; unknown URIs return 404. */
  private class FakePagingClient(pages: Map[String, String]) extends HttpSyncClient:
    override def config: HttpClientConfig         = Http.client
    override def send(request: Request): Response = pages
      .get(request.uri)
      .map(body => Response.ok.withJsonContent(body))
      .getOrElse(Response.notFound)

    override def noRetry: HttpSyncClient                              = this
    override def withMaxRetry(maxRetries: Int): HttpSyncClient        = this
    override def withConfig(config: HttpClientConfig): HttpSyncClient = this

  private def page(json: String): JSONObject = JSON.parse(json).asInstanceOf[JSONObject]

  private val firstResponse = page("""{"id":"q1","nextUri":"/p1","stats":{"state":"RUNNING"}}""")

  private val multiPage = Map(
    "/p1" ->
      """{"id":"q1","columns":[{"name":"id","type":"bigint"}],
        |"data":[[1],[2]],"nextUri":"/p2","stats":{"state":"RUNNING","processedRows":2}}"""
        .stripMargin,
    "/p2" -> """{"id":"q1","data":[[3],[4]],"nextUri":"/p3","stats":{"state":"RUNNING"}}""",
    "/p3" -> """{"id":"q1","stats":{"state":"FINISHED"}}"""
  )

  private val emptyResult = Map(
    "/p1" ->
      """{"id":"q1","columns":[{"name":"id","type":"bigint"}],"nextUri":"/p2","stats":{"state":"RUNNING"}}""",
    "/p2" -> """{"id":"q1","stats":{"state":"FINISHED"}}"""
  )

  private def newHandle(pages: Map[String, String]): TrinoQueryHandle =
    val handle =
      new TrinoQueryHandle(FakePagingClient(pages), trinoConfig, QueryProgressMonitor.noOp):
        // The best-effort DELETE would hit a real network address; not under test here
        override protected def sendCancelRequest(uri: String): Unit = ()
    handle.consume(firstResponse)
    handle

  test("should stream one batch per Trino page carrying column metadata") {
    val handle  = newHandle(multiPage)
    val batches = handle.batches().toList
    batches.size shouldBe 2
    batches.foreach(b => b.columns.map(_.name.name) shouldBe List("id"))
    batches(0).rows.map(_.values) shouldBe List(List(Some("1")), List(Some("2")))
    batches(1).rows.map(_.values) shouldBe List(List(Some("3")), List(Some("4")))
  }

  test("should materialize the same rows through await()") {
    val result = newHandle(multiPage).await()
    result.rowCount shouldBe 4
    result.rows.flatMap(_.values.flatten) shouldBe List("1", "2", "3", "4")
  }

  test("should reject await() once streaming has begun") {
    val handle = newHandle(multiPage)
    handle.batches()
    intercept[IllegalStateException] {
      handle.await()
    }
  }

  test("should reject a second batches() call — the stream is single-pass") {
    val handle = newHandle(multiPage)
    handle.batches()
    intercept[IllegalStateException] {
      handle.batches()
    }
  }

  test("should return the materialized result as a single batch after await()") {
    val handle = newHandle(multiPage)
    handle.await().rowCount shouldBe 4
    val batches = handle.batches().toList
    batches.size shouldBe 1
    batches.head.rowCount shouldBe 4
  }

  test("should yield a single zero-row batch carrying the schema for an empty result") {
    val batches = newHandle(emptyResult).batches().toList
    batches.size shouldBe 1
    batches.head.rowCount shouldBe 0
    batches.head.columns.map(_.name.name) shouldBe List("id")
  }

  test("should stop streaming at the next page boundary after cancel()") {
    val handle = newHandle(multiPage)
    val it     = handle.batches()
    it.hasNext shouldBe true
    it.next().rowCount shouldBe 2
    handle.cancel()
    it.hasNext shouldBe false
  }

end TrinoQueryHandleTest
