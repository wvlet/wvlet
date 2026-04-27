package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context

class CodeFormatterTest extends UniTest:
  import CodeFormatter.*
  test("format doc") {
    val doc = text("hello") + newline + text("world")
    val f   = CodeFormatter().format(doc)
    debug(f)
  }
