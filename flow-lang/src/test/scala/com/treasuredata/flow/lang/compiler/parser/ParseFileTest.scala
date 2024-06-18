package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class ParseFileTest extends AirSpec:
  test("parse primitive.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/standard/src/primitive.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }

  test("parse cdp_types.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/basic/src/cdp_types.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }

  test("parse behavior.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/basic/src/behavior.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }

  test("parse customer.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/basic/src/customer.flow"))
    val plan = p.parse()
    debug(plan.pp)
  }
