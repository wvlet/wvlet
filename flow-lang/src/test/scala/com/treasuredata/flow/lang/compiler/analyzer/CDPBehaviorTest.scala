package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.airspec.AirSpec

class CDPBehaviorTest extends AirSpec:

  test("cdp_behavior") {
    val result = Compiler.default("spec/cdp_behavior").compile()
    val resolvedPlan: List[LogicalPlan] = result
      .units
      .map: x =>
        debug(x.resolvedPlan.pp)
        x.resolvedPlan

    resolvedPlan shouldNotBe empty
  }
