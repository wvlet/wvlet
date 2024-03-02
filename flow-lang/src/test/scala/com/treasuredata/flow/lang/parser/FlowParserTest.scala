package com.treasuredata.flow.lang.parser

import com.treasuredata.flow.lang.CompileUnit
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser.parse("from A select _")

  test("parse behavior.flow"):
    val plan = FlowParser.parse(CompileUnit("examples/cdp_behavior/src/behavior.flow"))
    debug(plan)

  test("parse customer.flow"):
    val plan = FlowParser.parse(CompileUnit("examples/cdp_behavior/src/customer.flow"))
    // debug(plan)
    debug(plan)
