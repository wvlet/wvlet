package wvlet.lang.compiler.parser

import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.airspec.AirSpec

class WvletParserTest extends AirSpec:
  test("parse"):
    WvletParser(CompilationUnit.fromString("from A select _")).parse()

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

  test("tpch") {
    val plans = ParserPhase.parseSourceFolder("spec/tpch/src")
    plans.foreach: p =>
      debug(p.pp)
  }
