package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, DBType}
import wvlet.lang.compiler.codegen.SqlGenerator

abstract class SqlGeneratorSpec(path: String) extends AirSpec:
  private val name = path.split("\\/").lastOption.getOrElse(path)
  CompilationUnit
    .fromPath(path)
    .foreach { unit =>
      test(s"parse ${name}:${unit.sourceFile.fileName}") {
        debug(unit.sourceFile.getContentAsString)
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        trace(stmt.pp)
        val g   = SqlGenerator(DBType.DuckDB)
        val sql = g.print(stmt)
        debug(sql)
      }
    }

class TPCHGeneratorTest  extends SqlGeneratorSpec("spec/sql/tpc-h")
class TPCDSGeneratorTest extends SqlGeneratorSpec("spec/sql/tpc-ds")
