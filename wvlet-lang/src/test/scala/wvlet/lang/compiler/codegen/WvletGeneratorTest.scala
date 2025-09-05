package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.WvletGenerator
import wvlet.lang.compiler.transform.RewriteExpr

abstract class WvletGeneratorTest(path: String, ignoredSpec: Map[String, String] = Map.empty)
    extends AirSpec:
  private val testPrefix = path.split("\\/").lastOption.getOrElse(path)
  private val globalCtx  = Context.testGlobalContext(path)

  CompilationUnit
    .fromPath(path)
    .foreach { unit =>
      val file = unit.sourceFile.fileName
      test(s"Convert ${testPrefix}:${file} to Wvlet") {
        ignoredSpec.get(file).foreach(reason => ignore(reason))
        trace(s"[${file}]\n${unit.sourceFile.getContentAsString}")
        given ctx: Context = globalCtx.getContextOf(unit)
        val unresolvedPlan = ParserPhase.parse(unit, ctx)
        // Set the unresolved plan to the unit without resolving types
        unit.resolvedPlan = unresolvedPlan

        // Run basic expression rewrites
        RewriteExpr.run(unit, ctx)
        trace(unit.resolvedPlan.pp)

        val g = WvletGenerator()
        val d = g.convert(unit.resolvedPlan)
        // trace(d.pp)
        val wv = g.render(d)
        debug(s"[formatted ${file}]\n${wv}")

        // Test parsing the generated Wvlet query
        {
          val wvUnit            = CompilationUnit.fromWvletString(wv)
          given newCtx: Context = globalCtx.getContextOf(wvUnit)
          globalCtx.setContextUnit(wvUnit)
          val newPlan = ParserPhase.parse(wvUnit, newCtx)
          trace(newPlan.pp)
        }
      }
    }

end WvletGeneratorTest

class WvletGeneratorBasicSpec  extends WvletGeneratorTest("spec/basic")
class WvletGeneratorWvTPCHSPec extends WvletGeneratorTest("spec/tpch")

class WvletGeneratorTPCHSpec extends WvletGeneratorTest("spec/sql/tpc-h")
class WvletGeneratorSqlBasicSpec
    extends WvletGeneratorTest(
      "spec/sql/basic",
      ignoredSpec = Map(
        "decimal-literals.sql" -> "Need to decide how to support DECIMAL type in Wvlet"
      )
    )

class WvletGeneratorTPCDSSpec extends WvletGeneratorTest("spec/sql/tpc-ds")
