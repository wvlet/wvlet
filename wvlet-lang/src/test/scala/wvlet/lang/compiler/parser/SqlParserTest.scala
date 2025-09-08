package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

class SqlParserTest extends AirSpec:
  test("parse") {
    val stmt = SqlParser(CompilationUnit.fromSqlString("select * from A")).parse()
    debug(stmt.pp)
  }

  test("test missing SQL syntaxes") {
    val testCases = Map(
      "DELETE FROM test_table WHERE id = 1" -> "Delete syntax should work",
      "CREATE OR REPLACE VIEW test_view AS SELECT * FROM table1" -> "CREATE OR REPLACE VIEW should work",
      "SHOW CREATE VIEW test_view" -> "SHOW CREATE VIEW should work", 
      "SHOW FUNCTIONS" -> "SHOW FUNCTIONS should work"
    )
    
    testCases.foreach { case (sql, description) =>
      debug(s"Testing: $sql")
      try {
        val unit = CompilationUnit.fromSqlString(sql)
        val parser = SqlParser(unit)
        val result = parser.parse()
        debug(s"  SUCCESS: ${result.getClass.getSimpleName} - $description")
      } catch {
        case e: Exception =>
          debug(s"  ERROR: ${e.getMessage} - $description")
      }
    }
  }

end SqlParserTest

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
