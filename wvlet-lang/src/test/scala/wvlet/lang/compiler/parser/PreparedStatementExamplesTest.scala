package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

class PreparedStatementExamplesTest extends AirSpec:

  def parseStatement(sql: String): LogicalPlan =
    val parsed = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
    parsed match
      case PackageDef(_, statements, _, _) =>
        statements.head
      case other =>
        other

  test("Trino examples from issue") {
    // Test the examples from the GitHub issue
    val trinoExamples = List(
      "PREPARE my_select1 FROM SELECT * FROM nation",
      "PREPARE my_select1 FROM SELECT name FROM nation",
      "EXECUTE my_select1",
      "PREPARE my_select2 FROM SELECT name FROM nation WHERE regionkey = ? and nationkey < ?",
      "EXECUTE my_select2 USING 1, 3"
    )

    trinoExamples.foreach { sql =>
      info(s"Testing: $sql")
      val stmt = parseStatement(sql)
      debug(stmt.pp)
      // Just verify it parses without error
      if stmt == null then
        fail("Statement should not be null")
    }
  }

  test("DuckDB examples from issue") {
    // Test the examples from the GitHub issue
    val duckdbExamples = List(
      "PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, ?) AND age >= ?",
      "EXECUTE query_person('B', 40)",
      "PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, $2) AND age >= $1",
      "EXECUTE query_person(40, 'B')",
      "PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, $name_start_letter) AND age >= $minimum_age",
      "DEALLOCATE query_person"
    )

    duckdbExamples.foreach { sql =>
      info(s"Testing: $sql")
      val stmt = parseStatement(sql)
      debug(stmt.pp)
      // Just verify it parses without error
      if stmt == null then
        fail("Statement should not be null")
    }
  }