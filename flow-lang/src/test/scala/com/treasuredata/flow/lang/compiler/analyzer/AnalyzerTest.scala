package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  private val compiler = Compiler(List(Compiler.analysisPhases))

  test("analyze behavior plan") {
    val result = compiler.compile("examples/cdp_behavior/src/behavior/behavior.flow")
    result.typedPlans.collect:
      case p => debug(p.pp)
  }

  test("analyze customer plan") {
    val result = compiler.compile("examples/cdp_behavior/src/customer")
    result.typedPlans.collect:
      case p => debug(p.pp)
  }

  test("analyze behavior subscription") {
    val result = compiler.compile("examples/cdp_behavior/src/behavior")
    result.typedPlans.collect:
      case q: Query     => debug(q.pp)
      case s: Subscribe => debug(s.pp)
  }
