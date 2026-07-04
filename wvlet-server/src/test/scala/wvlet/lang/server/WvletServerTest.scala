package wvlet.lang.server

import wvlet.lang.BuildInfo
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.test.WvletDITest

class WvletServerTest extends WvletDITest:

  initDesign:
    _.add(WvletServer.testDesign)

  test("launch server") {
    val client = dep[FrontendRPC.RPCSyncClient]
    val status = client.FrontendApi.status
    status.version shouldBe BuildInfo.version
  }

  test("list flow runs over RPC") {
    val client = dep[FrontendRPC.RPCSyncClient]
    // The endpoint round-trips the flow-run DTOs over the wire. The working folder may or may
    // not hold recorded runs, so only the shape of the response is asserted
    val runs = client.FlowApi.listRuns(wvlet.lang.api.v1.flow.FlowApi.FlowRunListRequest())
    runs.foreach { r =>
      r.runId shouldNotBe empty
      r.flowName shouldNotBe empty
    }
  }

end WvletServerTest
