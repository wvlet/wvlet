package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.airspec.AirSpec

class TransformTest extends AirSpec:

  private val c = Compiler.default

  test("transform") {
    val result = c.compile("spec/cdp_behavior")
    val resolvedPlan: Option[LogicalPlan] = result
      .inFile("behavior.flow")
      .map: x =>
        debug(x.resolvedPlan.pp)
        x.resolvedPlan

    resolvedPlan shouldBe defined
  }
