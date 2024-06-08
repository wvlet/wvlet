package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.compiler.parser.Parsers
import wvlet.airspec.AirSpec

class ExecutionPlanTest extends AirSpec:

  test("create an execution plan") {
    val p = Parsers(CompilationUnit.fromString("select 1 + 1"))

    val logicalPlan = p.parse()
    debug(logicalPlan)
  }
