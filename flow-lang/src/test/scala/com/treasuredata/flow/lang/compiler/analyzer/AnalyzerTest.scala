package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{Compiler, Name}
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  private val compiler = Compiler(List(Compiler.analysisPhases))

  test("analyze stdlib") {
    val result     = compiler.compileSingle(Nil, ".", None)
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)
  }

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
    debug(result.context.scope.getAllEntries)

    val tpe = result.context.scope.lookupSymbol(Name.typeName("person"))
    debug(tpe.get)
  }
