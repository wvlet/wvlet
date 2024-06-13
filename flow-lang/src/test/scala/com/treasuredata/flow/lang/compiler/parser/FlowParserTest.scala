package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.airspec.AirSpec

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser(CompilationUnit.fromString("from A select _")).parse()

  test("parse behavior.flow"):
    pending("stabilizing the new parser")
    val plan = FlowParser(CompilationUnit.fromFile("examples/cdp_behavior/src/behavior.flow")).parse()
    debug(plan)

  test("parse customer.flow"):
    pending("stabilizing the new parser")
    val plan = FlowParser(CompilationUnit.fromFile("examples/cdp_behavior/src/customer.flow")).parse()
    // debug(plan)
    debug(plan)

  test("parse basic queries"):
    pending("stabilizing the new parser")
    val plan = ParserPhase.parseSourceFolder("examples/basic/src")
    plan.foreach: p =>
      debug(p)
