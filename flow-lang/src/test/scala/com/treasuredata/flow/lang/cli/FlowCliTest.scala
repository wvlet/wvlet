package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class FlowCliTest extends AirSpec:
  test("help") {
    FlowCli.main("--help")
  }

  test("query"):
    pending("stabilizing the new parser")
    FlowCli.main("compile examples/basic")
