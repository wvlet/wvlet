package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.WvletParser

/** Every standard library file must parse; a failure here breaks all stdlib function resolution */
class StdLibParseCheck extends UniTest:
  test("parse all stdlib files") {
    CompilationUnit
      .stdLib
      .foreach { unit =>
        try
          val plan = WvletParser(unit).parse()
          debug(s"parsed ${unit.sourceFile.fileName}")
        catch
          case e: Exception =>
            fail(s"Failed to parse ${unit.sourceFile.fileName}: ${e.getMessage}")
      }
  }

end StdLibParseCheck
