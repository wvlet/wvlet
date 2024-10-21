package wvlet.lang.server

import wvlet.airspec.AirSpec

class WvletServerMainTest extends AirSpec:
  test("run default command") {
    WvletServerMain.main("")
    WvletServerMain.main("-h")
  }

  test("run version") {
    WvletServerMain.main("version")
  }
