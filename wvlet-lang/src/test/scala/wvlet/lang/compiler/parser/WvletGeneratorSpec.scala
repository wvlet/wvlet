package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.WvletGenerator

abstract class WvletGeneratorSpec(path: String) extends AirSpec:
  private val name = path.split("\\/").lastOption.getOrElse(path)
  CompilationUnit
    .fromPath(path)
    .foreach { unit =>
      test(s"Convert ${name}:${unit.sourceFile.fileName} to Wvlet") {
        debug(unit.sourceFile.getContentAsString)
        val plan = ParserPhase.parse(unit, Context.NoContext)
        trace(plan.pp)
        val g  = WvletGenerator()
        val wv = g.print(plan)
        debug(wv)
      }
    }

end WvletGeneratorSpec

class WvletGeneratorBasicTest extends WvletGeneratorSpec("spec/basic")

class SqlToWvletTPCHGeneratorTest extends WvletGeneratorSpec("spec/sql/tpc-h")

class SqlToWvletTPCDSGeneratorTest extends WvletGeneratorSpec("spec/sql/tpc-ds")
