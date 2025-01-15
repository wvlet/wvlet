package wvlet.lang.compiler.formatter

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.{CompilationUnit, Context}

class CodeFormatterTest extends AirSpec:
  import CodeFormatter.*
  test("format doc") {
    val doc = text("hello") + newline + text("world")
    val f   = CodeFormatter().format(doc)
    debug(f)
  }
