package wvlet.lang.server

import wvlet.lang.api.v1.frontend.FrontendApi.QueryCancelRequest
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.test.WvletDITest
import wvlet.uni.util.ULID

/**
  * airframe-codec ships a built-in MessageCodec only for `wvlet.airframe.ulid.ULID`. This guards
  * the generic-codec fallback for `wvlet.uni.util.ULID` until the RPC stack itself migrates off
  * airframe-http (phases 0–4 of #1662).
  *
  * Lives in a dedicated spec because `QueryService` is `AutoCloseable`; sharing a spec with
  * `WvletServerTest` would shut its thread pool down between tests under per-test session scoping.
  */
class UlidRpcRoundTripTest extends WvletDITest:

  initDesign:
    _.add(WvletServer.testDesign)

  test("submitQuery returns a parseable ULID over RPC") {
    val client   = dep[FrontendRPC.RPCSyncClient]
    val request  = QueryRequest(query = "select 1")
    val response = client.FrontendApi.submitQuery(request)

    response.requestId shouldBe request.requestId
    ULID.isValid(response.queryId.toString) shouldBe true
  }

  test("getQueryInfo serves structured result rows over RPC") {
    val client   = dep[FrontendRPC.RPCSyncClient]
    val response = client
      .FrontendApi
      .submitQuery(QueryRequest(query = "select 42 as answer, 'rpc' as via"))

    val deadline        = System.currentTimeMillis() + 30000
    var info: QueryInfo = client
      .FrontendApi
      .getQueryInfo(QueryInfoRequest(response.queryId, pageToken = "0"))
    while !info.status.isFinished && System.currentTimeMillis() < deadline do
      Thread.sleep(50)
      info = client
        .FrontendApi
        .getQueryInfo(QueryInfoRequest(response.queryId, pageToken = info.pageToken))
    info.status.isFailed shouldBe false

    // The Seq[Seq[Any]] rows must survive the Weaver round-trip over the wire
    val result = info.result.getOrElse(throw new AssertionError("result was not populated"))
    result.schema.map(_.name) shouldBe List("answer", "via")
    result.rows.size shouldBe 1
    result.rows.head.map(v => Option(v).map(_.toString).orNull) shouldBe List("42", "rpc")
  }

  test("cancelQuery is exposed over RPC") {
    val client   = dep[FrontendRPC.RPCSyncClient]
    val response = client.FrontendApi.submitQuery(QueryRequest(query = "select 1"))
    val info     = client.FrontendApi.cancelQuery(QueryCancelRequest(response.queryId))
    info.status.isFinished shouldBe true
  }

end UlidRpcRoundTripTest
