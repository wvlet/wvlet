package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.Compiler
import wvlet.airspec.AirSpec

class TransformTest extends AirSpec:

  private val c = Compiler(List(Compiler.analysisPhases, Compiler.transformPhases))

  test("transform") {
    val result = c.compile("examples/cdp_behavior/src/behavior")
    val plans  = result.subscriptionPlans
    // plans shouldNotBe empty
    plans.foreach: plan =>
      debug(plan)
  }
