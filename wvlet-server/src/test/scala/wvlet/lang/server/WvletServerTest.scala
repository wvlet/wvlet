package wvlet.lang.server

import wvlet.airspec.AirSpec
import wvlet.lang.BuildInfo
import wvlet.lang.api.v1.frontend.FrontendApi.QueryRequest
import wvlet.lang.api.v1.frontend.FrontendRPC

class WvletServerTest extends AirSpec:

  initDesign:
    _.add(WvletServer.testDesign)

  test("launch server") { (client: FrontendRPC.RPCSyncClient) =>
    val status = client.FrontendApi.status()
    status.version shouldBe BuildInfo.version
  }
