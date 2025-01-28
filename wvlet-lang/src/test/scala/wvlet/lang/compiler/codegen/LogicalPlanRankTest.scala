package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.analyzer.LogicalPlanRank
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.{CompilationUnit, Context}

class LogicalPlanRankTest extends AirSpec:
  private val path      = "spec/sql/tpc-h"
  private val globalCtx = Context.testGlobalContext(path)

  CompilationUnit
    .fromPath(path)
    .foreach { unit =>
      test(s"Evaluate the readability of ${unit.sourceFile.fileName}") {

        given ctx: Context = globalCtx.getContextOf(unit)

        val sqlPlan = ParserPhase.parse(unit, ctx)

        trace(sqlPlan.pp)
        LogicalPlanRank.syntaxReadability(sqlPlan)

        val g  = WvletGenerator()
        val wv = g.print(sqlPlan)
        {
          val wvUnit = CompilationUnit.fromWvletString(wv)

          given newCtx: Context = globalCtx.getContextOf(wvUnit)

          globalCtx.setContextUnit(wvUnit)
          val wvPlan = ParserPhase.parse(wvUnit, newCtx)
          trace(wvPlan.pp)
          LogicalPlanRank.syntaxReadability(wvPlan)
        }
      }
    }

end LogicalPlanRankTest
