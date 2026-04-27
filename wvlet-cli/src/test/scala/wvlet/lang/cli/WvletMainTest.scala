package wvlet.lang.cli

import wvlet.uni.test.UniTest

class WvletMainTest extends UniTest:
  test("help") {
    WvletMain.main("--help")
    WvletMain.main("")
    WvletMain.main("version")
  }

  test("start server test") {
    WvletMain.main("ui --quit-immediately")
  }
