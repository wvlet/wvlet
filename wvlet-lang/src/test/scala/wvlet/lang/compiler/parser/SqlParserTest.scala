package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

class SqlParserTest extends AirSpec:
  test("parse") {
    val stmt = SqlParser(CompilationUnit.fromSqlString("select * from A")).parse()
    debug(stmt.pp)
  }

end SqlParserTest

class SqlParserBasicSpec extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/basic")
    .foreach { unit =>
      test(s"parse basic ${unit.sourceFile.fileName}") {
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        debug(stmt.pp)
      }
    }

class SqlParserTPCHSpec extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/tpc-h")
    .foreach { unit =>
      test(s"parse tpc-h ${unit.sourceFile.fileName}") {
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        debug(stmt.pp)
      }
    }

class SqlParserTPCDSSpec extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/tpc-ds")
    .foreach { unit =>
      test(s"parse tpc-ds ${unit.sourceFile.fileName}") {
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        debug(stmt.pp)
      }
    }
