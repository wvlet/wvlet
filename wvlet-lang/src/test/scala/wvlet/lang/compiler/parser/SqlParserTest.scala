package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit

class SqlParserTest extends AirSpec:
  test("parse") {
    val stmt = SqlParser(CompilationUnit.fromSqlString("select * from A")).parse()
    debug(stmt.pp)
  }

  test("parse tpch") {
    CompilationUnit
      .fromPath("spec/sql/tpc-h")
      .foreach { unit =>
        test(s"parse ${unit.sourceFile.fileName}") {
          val stmt = SqlParser(unit).parse()
          debug(stmt.pp)
        }
      }
  }
