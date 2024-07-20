package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class FlowCliTest extends AirSpec:
  test("help") {
    FlowCli.main("--help")
  }

  test("q1"):
    FlowCli.main("run spec/basic/src/q1.flow")

  test("q2"):
    FlowCli.main("run spec/basic/src/q2.flow")

  test("query"):
    FlowCli.main("run spec/basic/src/query.flow")

  test("model1-q1"):
    FlowCli.main("run spec/model1/src/q1.flow")

  test("tpch-q6"):
    pending(s"resolve list type functions (e.g., sum)")
    FlowCli.main("run spec/tpch/src/q6.flow")
