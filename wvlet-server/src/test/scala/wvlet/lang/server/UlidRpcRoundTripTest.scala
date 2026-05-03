package wvlet.lang.server

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.uni.util.ULID

/**
  * airframe-codec ships a built-in MessageCodec only for `wvlet.airframe.ulid.ULID`. This guards
  * the generic-codec fallback for `wvlet.uni.util.ULID` until the RPC stack itself migrates off
  * airframe-http (phases 0–4 of #1662).
  *
  * Lives in a dedicated spec because `QueryService` is `AutoCloseable`; sharing a spec with
  * `WvletServerTest` would shut its thread pool down between tests under AirSpec's per-test session
  * scoping.
  */
class UlidRpcRoundTripTest extends AirSpec:

  initDesign:
    _.add(WvletServer.testDesign)

  test("submitQuery returns a parseable ULID over RPC") { (client: FrontendRPC.RPCSyncClient) =>
    val request  = QueryRequest(query = "select 1")
    val response = client.FrontendApi.submitQuery(request)

    response.requestId shouldBe request.requestId
    ULID.isValid(response.queryId.toString) shouldBe true
  }
