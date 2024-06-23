package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class StdLibTest extends AirSpec:
  test("parse stdlib") {
    val units = CompilationUnit.fromPath("flow-lang/src/main/resources/flow-stdlib/src")
    units.foreach { u =>
      test(s"Parse ${u.sourceFile.fileName}") {
        val plan = FlowParser(u).parse()
        debug(plan.pp)
      }
    }
  }
