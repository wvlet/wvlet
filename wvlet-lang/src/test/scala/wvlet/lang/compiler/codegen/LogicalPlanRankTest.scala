package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.analyzer.LogicalPlanRank
import wvlet.lang.compiler.analyzer.LogicalPlanRank.ReadabilityScore
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context

class LogicalPlanRankTest extends AirSpec:

  def spec(path: String): Unit =
    val specName  = path.split("/").lastOption.getOrElse(path)
    val globalCtx = Context.testGlobalContext(path)

    var sqlScores: List[ReadabilityScore]   = Nil
    var wvletScores: List[ReadabilityScore] = Nil

    CompilationUnit
      .fromPath(path)
      .foreach { unit =>
        test(s"Evaluate the readability of ${specName}:${unit.sourceFile.fileName}") {

          given ctx: Context = globalCtx.getContextOf(unit)

          val sqlPlan = ParserPhase.parse(unit, ctx)

          trace(unit.sourceFile.getContentAsString)
          trace(sqlPlan.pp)
          val sqlScore = LogicalPlanRank.syntaxReadability(sqlPlan)
          sqlScores = sqlScore :: sqlScores

          val g  = WvletGenerator()
          val wv = g.print(sqlPlan)
          {
            val wvUnit = CompilationUnit.fromWvletString(wv)

            given newCtx: Context = globalCtx.getContextOf(wvUnit)

            globalCtx.setContextUnit(wvUnit)
            val wvPlan = ParserPhase.parse(wvUnit, newCtx)
            trace(wv)
            trace(wvPlan.pp)
            val wvScore = LogicalPlanRank.syntaxReadability(wvPlan)
            wvletScores = wvScore :: wvletScores
            debug(s"[${unit.sourceFile.fileName}]\nsql  : ${sqlScore.pp}\nwvlet: ${wvScore.pp}")
          }

        }
      }

    test(s"Report the readability of ${specName}") {
      // Report the average readability score
      val queries              = sqlScores.size
      val sqlAvg               = sqlScores.map(_.normalizedScore).sum / sqlScores.size
      val sqlAvgInversions     = sqlScores.map(_.inversionCount).sum.toDouble / sqlScores.size
      val sqlAvgLineMovement   = sqlScores.map(_.lineMovement).sum.toDouble / sqlScores.size
      val wvletAvg             = wvletScores.map(_.normalizedScore).sum / wvletScores.size
      val wvletAvgInversions   = wvletScores.map(_.inversionCount).sum.toDouble / wvletScores.size
      val wvletAvgLineMovement = wvletScores.map(_.lineMovement).sum.toDouble / wvletScores.size
      // report stats
      info(f"""Readability average report for ${specName} ${queries} queries:
           |[SQL]   score:${sqlAvg}%.2f, inversions:${sqlAvgInversions}%.2f, Line Movement:${sqlAvgLineMovement}%.2f
           |[Wvlet] score:${wvletAvg}%.2f, inversions:${wvletAvgInversions}%.2f, Line Movement:${wvletAvgLineMovement}%.2f
           |""".stripMargin)
    }

  end spec

  spec("spec/sql/tpc-h")
  spec("spec/sql/tpc-ds")

end LogicalPlanRankTest
