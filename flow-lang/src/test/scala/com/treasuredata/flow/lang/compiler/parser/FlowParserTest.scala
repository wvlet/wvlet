package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.airspec.AirSpec

class FlowParserTest extends AirSpec:
  test("parse"):
    FlowParser(CompilationUnit.fromString("from A select _")).parse()

  test("parse basic queries"):
    val plans = ParserPhase.parseSourceFolder("spec/basic/src")
    plans.foreach: p =>
      debug(p.pp)

  test("parse cdp_simple queries"):
    val plans = ParserPhase.parseSourceFolder("spec/cdp_simple/src")
    plans.foreach: p =>
      debug(p.pp)

  test("parse cdp_behavior queries"):
    val plans = ParserPhase.parseSourceFolder("spec/cdp_behavior/src")
    plans.foreach: p =>
      debug(p.pp)
