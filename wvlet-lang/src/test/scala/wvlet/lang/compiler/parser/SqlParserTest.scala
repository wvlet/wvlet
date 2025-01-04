package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit

class SqlParserTest extends AirSpec:
  test("parse") {
    val stmt = SqlParser(CompilationUnit.fromSqlString("select * from A")).parse()
    debug(stmt.pp)
  }
