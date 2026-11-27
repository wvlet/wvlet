package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.QueryCancel
import wvlet.lang.compiler.connector.QueryState
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.uni.http.Http
import wvlet.uni.http.HttpClientConfig
import wvlet.uni.http.HttpSyncClient
import wvlet.uni.http.Request
import wvlet.uni.http.Response
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.test.UniTest

import scala.scalajs.js

/**
  * Ctrl-C in the npm REPL raises the [[QueryCancel]] flag while a Trino query pages its results.
  * The handle then stops at the next page and asks Trino to abort the query
  */
class TrinoQueryCancelTest extends UniTest:

  private class FakePagingClient extends HttpSyncClient:
    var requested: List[String]                   = Nil
    override def config: HttpClientConfig         = Http.client
    override def send(request: Request): Response =
      requested = request.uri :: requested
      Response
        .ok
        .withJsonContent(
          """{"id":"q1","data":[[1]],"nextUri":"/next","stats":{"state":"RUNNING"}}"""
        )

    override def noRetry: HttpSyncClient                              = this
    override def withMaxRetry(maxRetries: Int): HttpSyncClient        = this
    override def withConfig(config: HttpClientConfig): HttpSyncClient = this

  test("stop paging and ask Trino to abort when the cancel flag is raised") {
    val client  = FakePagingClient()
    var deleted = List.empty[String]
    val handle  =
      new TrinoQueryHandle(
        client,
        TrinoConfig(host = "localhost", user = "test"),
        QueryProgressMonitor.noOp
      ):
        override protected def sendCancelRequest(uri: String): Unit = deleted = uri :: deleted
    handle.consume(
      JSON
        .parse("""{"id":"q1","nextUri":"/next","stats":{"state":"RUNNING"}}""")
        .asInstanceOf[JSONObject]
    )

    QueryCancel.arm()
    js.Dynamic
      .global
      .Atomics
      .store(js.Dynamic.newInstance(js.Dynamic.global.Int32Array)(QueryCancel.sharedBuffer), 0, 1)
    try
      val e = intercept[WvletLangException] {
        handle.await()
      }
      e.statusCode shouldBe StatusCode.QUERY_CANCELLED
    finally
      QueryCancel.disarm()

    client.requested shouldBe Nil
    deleted shouldBe List("/next")
    handle.state shouldBe QueryState.Canceled
  }

end TrinoQueryCancelTest
