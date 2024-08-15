package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class FlowREPLTest extends AirSpec:
  test("help") {
    FlowREPLCli.main("-c 'help'")
  }

  test("model in the working folder") {
    FlowREPLCli.main("-w spec/model1 -c 'from person_filter(2)'")
  }

  test("def new model") {
    FlowREPLCli
      .main("-w spec/model1 -c 'model m(v:int) = from person where id = v end' -c 'from m(1)'")
  }

  test("limit shown rows") {
    FlowREPLCli
      .main("""-c "from 'https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'" """)
  }

  test("show models") {
    FlowREPLCli.main("-w spec/model1 -c 'show models'")
    FlowREPLCli.main("-w spec/model1 -c 'show models limit 1'")
    FlowREPLCli.main("-w spec/model1 -c 'show models' -c 'show models limit 5'")
  }

  test("select group index") {
    FlowREPLCli.main("-w spec/model1 -c 'from person group by age / 10 select _1'")
  }

end FlowREPLTest
