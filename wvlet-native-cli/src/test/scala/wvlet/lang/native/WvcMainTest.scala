package wvlet.lang.native

import wvlet.airspec.AirSpec

class WvcMainTest extends AirSpec:
  test("run command") {
    WvcMain.main(Array("-q", "select 1"))
  }

  test("use stdlib") {
    WvcMain.main(Array("-q", "select '1'.to_int"))
  }

  test("load spec") {
    WvcMain.main(Array("-l", "debug", "-w", "spec/basic", "-q", "from person"))
  }
