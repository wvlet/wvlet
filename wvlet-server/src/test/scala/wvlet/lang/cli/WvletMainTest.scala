package wvlet.lang.cli

import wvlet.airspec.AirSpec

class WvletMainTest extends AirSpec:
  test("run default command") {
    WvletMain.main("")
    WvletMain.main("-h")
  }

  test("run version") {
    WvletMain.main("version")
  }
