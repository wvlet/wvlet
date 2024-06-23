package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class ParseFileTest extends AirSpec:
  test("parse cdp_types.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/cdp_basic/src/cdp_types.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }

  test("parse behavior.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/cdp_basic/src/behavior.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }

  test("parse customer.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/cdp_basic/src/customer.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }
