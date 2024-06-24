package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.Compiler
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  private val compiler = Compiler(List(Compiler.analysisPhases))

  test("analyze cdp_simple plan") {
    val result     = compiler.compile("spec/cdp_simple")
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)
  }

  test("analyze basic") {
    val result     = compiler.compile("spec/basic")
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      debug(p.pp)
    debug(result.context.scope.getAllTypes)

    val tpe = result.context.scope.findType("person")
    debug(tpe.get.typeDescription)

  }
