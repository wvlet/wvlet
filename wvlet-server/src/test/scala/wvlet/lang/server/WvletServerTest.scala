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

end WvletServerTest
