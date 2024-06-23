package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class FlowCliTest extends AirSpec:
  test("help") {
    FlowCli.main("--help")
  }

  test("query"):
    FlowCli.main("compile spec/basic")
