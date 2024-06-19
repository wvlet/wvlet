package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  pendingUntil("Stabilizing the new parser")

  private val compiler = Compiler(List(Compiler.analysisPhases))

  test("analyze behavior plan") {
    val result = compiler.compile("examples/cdp_behavior")
    result
      .typedPlans
      .collect:
        case p =>
          debug(p.pp)
  }
