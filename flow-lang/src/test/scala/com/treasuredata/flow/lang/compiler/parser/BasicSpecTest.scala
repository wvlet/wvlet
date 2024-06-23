package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class BasicSpecTest extends AirSpec:
  val units = CompilationUnit.fromPath("spec/basic/src")
  for u <- units do
    test(s"parse ${u.sourceFile.fileName}") {
      val p    = FlowParser(u)
      val plan = p.parse()
      debug(plan.pp)
    }
