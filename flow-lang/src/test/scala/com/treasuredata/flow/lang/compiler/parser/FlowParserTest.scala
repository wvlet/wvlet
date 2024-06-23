package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.airspec.AirSpec

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser(CompilationUnit.fromString("from A select _")).parse()

  test("parse behavior.flow"):
    val plan = FlowParser(CompilationUnit.fromFile("spec/cdp_behavior/src/behavior.flow")).parse()
    debug(plan.pp)

  test("parse customer.flow"):
    val plan = FlowParser(CompilationUnit.fromFile("spec/cdp_behavior/src/customer.flow")).parse()
    debug(plan.pp)

  test("parse basic queries"):
    val plan = ParserPhase.parseSourceFolder("spec/basic/src")
    plan.foreach: p =>
      debug(p.pp)
