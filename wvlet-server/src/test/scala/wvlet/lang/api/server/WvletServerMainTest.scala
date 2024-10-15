package wvlet.lang.api.server

import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class WvletServerMainTest extends AirSpec:

  test("show version") {
    WvletServerMain.main("")
    WvletServerMain.main("version")
  }
