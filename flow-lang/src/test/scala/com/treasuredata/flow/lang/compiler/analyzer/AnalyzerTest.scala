package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  private val compiler = Compiler(List(Compiler.analysisPhases))

  test("analyze cdp-basic plan") {
    val result = compiler.compile("spec/cdp-basic")
    result
      .typedPlans
      .collect:
        case p =>
          debug(p.pp)
  }
