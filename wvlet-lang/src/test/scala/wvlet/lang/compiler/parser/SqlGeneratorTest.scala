package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, DBType}
import wvlet.lang.compiler.codegen.SqlGenerator

class SqlGeneratorTest extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/tpc-h")
    .foreach { unit =>
      test(s"parse tpc-h:${unit.sourceFile.fileName}") {
        debug(unit.sourceFile.getContentAsString)
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        trace(stmt.pp)
        val g   = SqlGenerator(DBType.DuckDB)
        val sql = g.print(stmt)
        debug(sql)
      }
    }

  CompilationUnit
    .fromPath("spec/sql/tpc-ds")
    .foreach { unit =>
      test(s"parse tpc-ds:${unit.sourceFile.fileName}") {
        debug(unit.sourceFile.getContentAsString)
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        trace(stmt.pp)
        val g   = SqlGenerator(DBType.DuckDB)
        val sql = g.print(stmt)
        debug(sql)
      }
    }
