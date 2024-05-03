package com.treasuredata.flow.lang.compiler.transform

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.airspec.AirSpec

class TransformTest extends AirSpec:

  private val c = Compiler.default

  test("transform") {
    val result = c.compile("examples/cdp_behavior")
    val subscriptions: List[LogicalPlan] = result
      .inFile("behavior.flow")
      .map: x =>
        // debug(x.resolvedPlan.pp)
        x.subscriptionPlans
      .getOrElse(Nil)

    subscriptions shouldNotBe empty
    subscriptions.foreach: plan =>
      debug(plan.pp)
  }
