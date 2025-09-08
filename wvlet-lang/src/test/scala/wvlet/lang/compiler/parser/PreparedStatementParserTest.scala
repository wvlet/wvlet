package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

class PreparedStatementParserTest extends AirSpec:

  def parseStatement(sql: String): LogicalPlan =
    val parsed = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
    parsed match
      case PackageDef(_, statements, _, _) =>
        statements.head
      case other =>
        other

  test("parse PREPARE statement with FROM keyword (Trino style)") {
    val sql = "PREPARE my_select1 FROM SELECT * FROM nation"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case prepareStmt: PrepareStatement =>
        prepareStmt.name.leafName shouldBe "my_select1"
      case _ =>
        fail(s"Expected PrepareStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse PREPARE statement with AS keyword (DuckDB style)") {
    val sql = "PREPARE query_person AS SELECT * FROM person WHERE name = ?"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case prepareStmt: PrepareStatement =>
        prepareStmt.name.leafName shouldBe "query_person"
      case _ =>
        fail(s"Expected PrepareStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse EXECUTE statement without parameters") {
    val sql = "EXECUTE my_select1"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case executeStmt: ExecuteStatement =>
        executeStmt.name.leafName shouldBe "my_select1"
        executeStmt.parameters shouldBe empty
      case _ =>
        fail(s"Expected ExecuteStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse EXECUTE statement with USING parameters (Trino style)") {
    val sql = "EXECUTE my_select2 USING 1, 3"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case executeStmt: ExecuteStatement =>
        executeStmt.name.leafName shouldBe "my_select2"
        executeStmt.parameters.size shouldBe 2
      case _ =>
        fail(s"Expected ExecuteStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse EXECUTE statement with parentheses parameters (DuckDB style)") {
    val sql = "EXECUTE query_person('B', 40)"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case executeStmt: ExecuteStatement =>
        executeStmt.name.leafName shouldBe "query_person"
        executeStmt.parameters.size shouldBe 2
      case _ =>
        fail(s"Expected ExecuteStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse DEALLOCATE statement") {
    val sql = "DEALLOCATE query_person"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case deallocateStmt: DeallocateStatement =>
        deallocateStmt.name.leafName shouldBe "query_person"
      case _ =>
        fail(s"Expected DeallocateStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse complex PREPARE statement with parameters") {
    val sql = "PREPARE my_select2 FROM SELECT name FROM nation WHERE regionkey = ? and nationkey < ?"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case prepareStmt: PrepareStatement =>
        prepareStmt.name.leafName shouldBe "my_select2"
      case _ =>
        fail(s"Expected PrepareStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse multiple statements") {
    val sql = """
      PREPARE my_select1 FROM SELECT * FROM nation;
      EXECUTE my_select1;
      DEALLOCATE my_select1
    """
    val parsed = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
    debug(parsed.pp)
    
    parsed match
      case PackageDef(_, statements, _, _) =>
        statements.size shouldBe 3
        
        statements(0) match
          case _: PrepareStatement => // ok
          case other => fail(s"Expected PrepareStatement, got ${other.getClass.getSimpleName}")
        
        statements(1) match
          case _: ExecuteStatement => // ok
          case other => fail(s"Expected ExecuteStatement, got ${other.getClass.getSimpleName}")
        
        statements(2) match
          case _: DeallocateStatement => // ok
          case other => fail(s"Expected DeallocateStatement, got ${other.getClass.getSimpleName}")
      case _ =>
        fail(s"Expected PackageDef, got ${parsed.getClass.getSimpleName}")
  }

  test("parse PREPARE with DuckDB $1 style parameters") {
    val sql = "PREPARE query_person AS SELECT * FROM person WHERE age >= $1 AND name = $2"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case prepareStmt: PrepareStatement =>
        prepareStmt.name.leafName shouldBe "query_person"
      case _ =>
        fail(s"Expected PrepareStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse PREPARE with DuckDB named parameters") {
    val sql = "PREPARE query_person AS SELECT * FROM person WHERE age >= $minimum_age AND name = $name_start"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case prepareStmt: PrepareStatement =>
        prepareStmt.name.leafName shouldBe "query_person"
      case _ =>
        fail(s"Expected PrepareStatement, got ${stmt.getClass.getSimpleName}")
  }

  test("parse EXECUTE with named parameters (DuckDB style)") {
    val sql = "EXECUTE query_person(40, 'B')"
    val stmt = parseStatement(sql)
    debug(stmt.pp)
    
    stmt match
      case executeStmt: ExecuteStatement =>
        executeStmt.name.leafName shouldBe "query_person"
        executeStmt.parameters.size shouldBe 2
      case _ =>
        fail(s"Expected ExecuteStatement, got ${stmt.getClass.getSimpleName}")
  }