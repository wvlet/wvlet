package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{Compiler, Name}
import com.treasuredata.flow.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  test("analyze stdlib") {
    val result     = Compiler.default("spec/empty").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)

    val units = result.context.global.getAllCompilationUnits
    units shouldNotBe empty
    val files = units.map(_.sourceFile.fileName)
    files shouldContain "int.flow"
    files shouldContain "string.flow"
  }

  test("analyze cdp_simple plan") {
    val result     = Compiler.default("spec/cdp_simple").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)
  }

  test("analyze basic") {
    val result     = Compiler.default("spec/basic").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      debug(p.pp)
    debug(result.context.scope.getAllEntries)

    val tpe = result.context.scope.lookupSymbol(Name.typeName("person"))
    debug(tpe.get)
  }

end AnalyzerTest
