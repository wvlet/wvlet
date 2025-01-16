package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.WvletGenerator

abstract class WvletGeneratorTest(path: String) extends AirSpec:
  private val testPrefix = path.split("\\/").lastOption.getOrElse(path)
  private val globalCtx  = Context.testGlobalContext(path)

  CompilationUnit
    .fromPath(path)
    .foreach { unit =>
      val file = unit.sourceFile.fileName
      test(s"Convert ${testPrefix}:${file} to Wvlet") {
        debug(s"[${file}]\n${unit.sourceFile.getContentAsString}")
        given ctx: Context = globalCtx.getContextOf(unit)
        val plan           = ParserPhase.parse(unit, ctx)
        trace(plan.pp)
        val g  = WvletGenerator()
        val wv = g.print(plan)
        debug(s"[formatted]\n${wv}")
      }
    }

end WvletGeneratorTest

class WvletGeneratorBasicSpec extends WvletGeneratorTest("spec/basic")

class WvletGeneratorTPCHSpec extends WvletGeneratorTest("spec/sql/tpc-h")

class WvletGeneratorTPCDSSpec extends WvletGeneratorTest("spec/sql/tpc-ds")
