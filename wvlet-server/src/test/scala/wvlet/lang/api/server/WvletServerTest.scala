package wvlet.lang.api.server

import wvlet.airspec.AirSpec
import wvlet.lang.api.v1.frontend.FrontendRPC

class WvletServerTest extends AirSpec:

  initDesign(_.add(WvletServer.testDesign))

  test("launch server") { (client: FrontendRPC.RPCSyncClient) =>
    debug(WvletServer.router)

    val status = client.FrontendApi.status()
    debug(status)
  }
