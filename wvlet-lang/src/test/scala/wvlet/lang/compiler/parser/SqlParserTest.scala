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
    val testCases = List(
      ("DELETE FROM test_table WHERE id = 1", "Delete"),
      ("CREATE OR REPLACE VIEW test_view AS SELECT * FROM table1", "CreateView"),
      ("SHOW CREATE VIEW test_view", "Show"),
      ("SHOW FUNCTIONS", "Show")
    )

    testCases.foreach { case (sql, expectedType) =>
      debug(s"Testing: $sql")
      val unit   = CompilationUnit.fromSqlString(sql)
      val parser = SqlParser(unit)
      val result = parser.parse()
      debug(s"  SUCCESS: ${result.getClass.getSimpleName}")

      // Extract the actual statement from PackageDef
      result match
        case pkg: PackageDef =>
          pkg.statements.headOption match
            case Some(stmt) =>
              val stmtType = stmt.getClass.getSimpleName
              debug(s"    Actual statement type: $stmtType")
              stmtType shouldBe expectedType
            case None =>
              fail(s"No statements found in package for: $sql")
        case _ =>
          fail(s"Expected PackageDef but got: ${result.getClass.getSimpleName}")
    }
  }

  test("comprehensive SQL syntax tests") {
    val complexCases = List(
      "CREATE OR REPLACE VIEW sales_summary AS SELECT region, SUM(amount) as total_sales FROM sales WHERE year = 2024 GROUP BY region ORDER BY total_sales DESC",
      "CREATE VIEW user_details AS SELECT id, name, email FROM users",
      "DROP VIEW sales_summary",
      "DELETE FROM orders WHERE status = 'cancelled'",
      "DELETE FROM products WHERE price < 10 AND category = 'outdated'"
    )

    complexCases.foreach { sql =>
      debug(s"Testing complex: $sql")
      val unit   = CompilationUnit.fromSqlString(sql)
      val parser = SqlParser(unit)
      val result = parser.parse()
      debug(s"  SUCCESS: Parsed as ${result.getClass.getSimpleName}")

      result match
        case pkg: PackageDef =>
          pkg.statements.size shouldBe 1
          debug(s"    Statement: ${pkg.statements.head.getClass.getSimpleName}")
        case _ =>
          fail(s"Expected PackageDef but got: ${result.getClass.getSimpleName}")
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
