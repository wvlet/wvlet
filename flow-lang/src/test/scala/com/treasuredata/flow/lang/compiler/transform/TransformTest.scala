package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.Subscribe
import wvlet.airspec.AirSpec

class TransformTest extends AirSpec:

  private val c = Compiler(List(Compiler.analysisPhases, Compiler.transformPhases))

  test("transform") {
    val result = c.compile("examples/cdp_behavior/src/behavior")
    result.typedPlans
      .collectFirst:
        case s: Subscribe if s.name.value == "behavior_weblogs_1h" => s
      .foreach: (s: Subscribe) =>
        debug(s)

  }
