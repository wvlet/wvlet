package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class FlowREPLTest extends AirSpec:
  test("help") {
    FlowCli.main("repl -c 'help'")
  }

  test("model in the working folder") {
    FlowCli.main("repl -w spec/model1 -c 'from person'")
  }
