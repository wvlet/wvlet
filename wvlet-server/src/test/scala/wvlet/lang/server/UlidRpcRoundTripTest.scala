package wvlet.lang.server

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.uni.util.ULID

/**
  * Verifies that `wvlet.uni.util.ULID` survives a round-trip through the still-airframe-http RPC
  * stack. airframe-codec only ships a built-in MessageCodec for `wvlet.airframe.ulid.ULID`, so this
  * test guards against regressions in the generic-codec fallback while we are still on
  * airframe-http during phases 0–4 of the migration.
  */
class UlidRpcRoundTripTest extends AirSpec:

  initDesign:
    _.add(WvletServer.testDesign)

  test("submitQuery returns a parseable ULID over RPC") { (client: FrontendRPC.RPCSyncClient) =>
    val request  = QueryRequest(query = "select 1")
    val response = client.FrontendApi.submitQuery(request)

    // Round-trip must preserve a valid ULID, not produce a null-backed instance
    response.queryId shouldNotBe null
    ULID.isValid(response.queryId.toString) shouldBe true
    response.requestId shouldNotBe null
    ULID.isValid(response.requestId.toString) shouldBe true
    // requestId from the request must come back unchanged
    response.requestId shouldBe request.requestId
  }
