package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*

class InsertIntoParserTest extends AirSpec:

  def parseInsert(sql: String): LogicalPlan =
    val parsed = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
    parsed match
      case PackageDef(_, statements, _, _) =>
        statements.head
      case other =>
        other

  def getTableName(target: TableOrFileName): String =
    target match
      case q: QualifiedName =>
        q.fullName
      case s: StringLiteral =>
        s.stringValue
      case _ =>
        throw new Exception(s"Unexpected target type: ${target}")

  def getColumnNames(columns: List[NameExpr]): List[String] = columns.map {
    case i: Identifier =>
      i.unquotedValue
    case q: QualifiedName =>
      q.fullName
    case _ =>
      throw new Exception("Expected Identifier or QualifiedName")
  }

  test("parse INSERT INTO with VALUES") {
    val sql  = "INSERT INTO users VALUES (1, 'Alice', 25)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child.isInstanceOf[Values] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with column list") {
    val sql  = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        getColumnNames(columns) shouldBe List("id", "name", "age")
        child.isInstanceOf[Values] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with SELECT") {
    val sql  = "INSERT INTO users SELECT * FROM temp_users"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child.isInstanceOf[Project] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with column list and SELECT") {
    val sql  = "INSERT INTO users (id, name) SELECT id, name FROM temp_users"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        getColumnNames(columns) shouldBe List("id", "name")
        child.isInstanceOf[Project] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with multi-row VALUES") {
    val sql  = "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child match
          case v: Values =>
            debug(s"Values has ${v.rows.size} rows")
            v.rows.foreach(row => debug(s"Row: ${row}"))
            v.rows.size shouldBe 3
          case _ =>
            fail(s"Expected Values, got ${child}")
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with qualified table name") {
    val sql  = "INSERT INTO catalog.schema.users VALUES (1, 'Alice', 25)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "catalog.schema.users"
        columns shouldBe Nil
        child.isInstanceOf[Values] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with complex expressions") {
    val sql  = "INSERT INTO users VALUES (1, UPPER('alice'), CURRENT_DATE)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child.isInstanceOf[Values] shouldBe true
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with subquery in SELECT") {
    val sql  = """
      INSERT INTO summary_table
      SELECT category, COUNT(*) as cnt
      FROM (
        SELECT * FROM products WHERE price > 100
      ) t
      GROUP BY category
    """
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _) =>
        getTableName(target) shouldBe "summary_table"
        columns shouldBe Nil
        // The child should be a more complex query plan
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

end InsertIntoParserTest
