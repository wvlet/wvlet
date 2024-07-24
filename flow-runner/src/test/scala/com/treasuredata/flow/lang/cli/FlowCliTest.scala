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

  for i <- 1 to 22 do
    test(s"tpch-q${i}"):
      info(s"Running tpch-q${i}")
      if i == 12 then
        pending("support multiple function bindings with different args")
      if i == 18 then
        pending("support multiple function bindings with different args")
      if i == 19 then
        pending("support multiple function bindings with different args")
      if i == 20 then
        pending("support multiple function bindings with different args")
      if i == 22 then
        pending("function chain substring(...).in(...)")
      FlowCli.main(s"run --tpch spec/tpch/src/q${i}.flow")

end FlowCliTest
