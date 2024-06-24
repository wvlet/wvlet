package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.airspec.AirSpec

class CDPBehaviorTest extends AirSpec:

  private val c = Compiler.default
  test("cdp_behavior") {
    val result = c.compile("spec/cdp_behavior")
    val resolvedPlan: List[LogicalPlan] = result
      .units
      .map: x =>
        debug(x.resolvedPlan.pp)
        x.resolvedPlan

    resolvedPlan shouldNotBe empty
  }
