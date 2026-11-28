package wvlet.lang.server

import wvlet.lang.api.v1.frontend.FrontendApi.QueryCancelRequest
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.test.WvletDITest

/**
  * Query ids (strings) and request ids (`wvlet.uni.util.ULID`) must survive the RPC round-trip.
  *
  * Lives in a dedicated spec because `QueryService` is `AutoCloseable`; sharing a spec with
  * `WvletServerTest` would shut its thread pool down between tests under per-test session scoping.
  */
class QueryIdRpcRoundTripTest extends WvletDITest:

  initDesign:
    _.add(WvletServer.testDesign)

  test("submitQuery returns a query id and the request id over RPC") {
    val client   = dep[FrontendRPC.RPCSyncClient]
    val request  = QueryRequest(query = "select 1")
    val response = client.FrontendApi.submitQuery(request)

    response.requestId shouldBe request.requestId
    QueryIdGenerator.isQueryId(response.queryId) shouldBe true
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

  test("resubmitting a request id returns the same query over RPC") {
    val client  = dep[FrontendRPC.RPCSyncClient]
    val request = QueryRequest(query = "select 1")
    val first   = client.FrontendApi.submitQuery(request)
    val again   = client.FrontendApi.submitQuery(request)
    again.queryId shouldBe first.queryId
  }

  test("status reports query counts over RPC") {
    val client = dep[FrontendRPC.RPCSyncClient]
    val status = client.FrontendApi.status
    (status.runningQueries >= 0) shouldBe true
    (status.queuedQueries >= 0) shouldBe true
  }

  test("cancelQuery is exposed over RPC") {
    val client   = dep[FrontendRPC.RPCSyncClient]
    val response = client.FrontendApi.submitQuery(QueryRequest(query = "select 1"))
    val info     = client.FrontendApi.cancelQuery(QueryCancelRequest(response.queryId))
    info.status.isFinished shouldBe true
  }

end QueryIdRpcRoundTripTest
