package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class ParseFileTest extends AirSpec:
  test("parse primitive.flow") {
    val p    = FlowParser(CompilationUnit.fromFile("spec/standard/src/primitive.flow"))
    val plan = p.parse()
    debug(plan)
  }
