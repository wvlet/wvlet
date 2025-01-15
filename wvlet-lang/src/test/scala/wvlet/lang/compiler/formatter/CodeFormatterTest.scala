package wvlet.lang.compiler.formatter

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.{CompilationUnit, Context}

class CodeFormatterTest extends AirSpec:
  CompilationUnit
    .fromPath("spec/basic")
    .foreach { unit =>
      test(s"Format ${unit.sourceFile.fileName}") {
        debug(s"[${unit.sourceFile.fileName}]\n${unit.sourceFile.getContentAsString}")
        val plan = ParserPhase.parse(unit, Context.NoContext)
        trace(plan.pp)
        val f  = WvletFormatter()
        val d = f.convert(plan)
        debug(s"[doc]\n${d}")
        val wv = f.render(0, d)
        debug(s"[formatted]\n${wv}")
      }
    }
