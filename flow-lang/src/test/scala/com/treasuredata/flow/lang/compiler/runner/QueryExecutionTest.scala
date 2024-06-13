package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.compiler.runner.PlanExecutor
import wvlet.airspec.AirSpec

class QueryExecutionTest extends AirSpec:
  pendingUntil("Stabilizing the new parser")

  test("run basic query") {
    val executor = PlanExecutor()
    val result   = Compiler.default.compile("examples/basic")
    result
      .inFile("q1.flow")
      .map: u =>
        executor.execute(u, result.context)
  }
