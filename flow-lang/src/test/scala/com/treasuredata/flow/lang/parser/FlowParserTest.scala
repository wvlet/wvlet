package com.treasuredata.flow.lang.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser.parse("from A select _")

  test("parse behavior.flow"):
    val plan = FlowParser.parse(CompilationUnit.fromFile("examples/cdp_behavior/src/behavior/behavior.flow"))
    debug(plan)

  test("parse customer.flow"):
    val plan = FlowParser.parse(CompilationUnit.fromFile("examples/cdp_behavior/src/customer/customer.flow"))
    // debug(plan)
    debug(plan)

  test("parse basic queries"):
    val plan = FlowParser.parseSourceFolder("examples/basic/src")
    plan.foreach: p =>
      debug(p)
