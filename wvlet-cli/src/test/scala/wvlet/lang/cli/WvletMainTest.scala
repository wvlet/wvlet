package wvlet.lang.cli

import wvlet.airspec.AirSpec

class WvletMainTest extends AirSpec:
  test("help") {
    WvletMain.main("--help")
    WvletMain.main("")
    WvletMain.main("version")
  }

  test("start server test") {
    WvletMain.main("ui --quit-immediately")
  }
