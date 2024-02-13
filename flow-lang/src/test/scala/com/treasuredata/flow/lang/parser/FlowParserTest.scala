package com.treasuredata.flow.lang.parser

import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser.parse("from A select _")

  test("parse behavior.flow"):
    val src = IOUtil.readAsString("examples/cdp_behavior/src/behavior.flow")
    FlowParser.parse(src)
