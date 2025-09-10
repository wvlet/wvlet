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

  def getColumnNames(columns: List[NameExpr]): List[String] = columns.map {
    case i: Identifier =>
      i.unquotedValue
    case q: QualifiedName =>
      q.fullName
  }

  test("parse INSERT INTO with VALUES") {
    val sql  = "INSERT INTO users VALUES (1, 'Alice', 25)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child match
          case v: Values =>
            v.rows.size shouldBe 1
            // Verify the structure of the first row
            v.rows.head match
              case arr: ArrayConstructor =>
                arr.values.size shouldBe 3
                // Check first value is a number literal
                arr.values(0).isInstanceOf[LongLiteral] shouldBe true
                // Check second value is a string literal
                arr.values(1).isInstanceOf[SingleQuoteString] shouldBe true
                // Check third value is a number literal
                arr.values(2).isInstanceOf[LongLiteral] shouldBe true
              case _ =>
                fail("Expected ArrayConstructor for VALUES row")
          case _ =>
            fail(s"Expected Values, got ${child.getClass.getSimpleName}")
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

  test("parse INSERT INTO with column list") {
    val sql  = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"
    val plan = parseInsert(sql)
    debug(plan.pp)

    plan match
      case InsertInto(target, columns, child, _, _) =>
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
      case InsertInto(target, columns, child, _, _) =>
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
      case InsertInto(target, columns, child, _, _) =>
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
      case InsertInto(target, columns, child, _, _) =>
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
      case InsertInto(target, columns, child, _, _) =>
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
      case InsertInto(target, columns, child, _, _) =>
        getTableName(target) shouldBe "users"
        columns shouldBe Nil
        child match
          case v: Values =>
            v.rows.size shouldBe 1
            v.rows.head match
              case arr: ArrayConstructor =>
                arr.values.size shouldBe 3
                // Check first value is a literal
                arr.values(0).isInstanceOf[LongLiteral] shouldBe true
                // Check second value is a function call (UPPER)
                arr.values(1).isInstanceOf[FunctionApply] shouldBe true
                // Check third value is an identifier (CURRENT_DATE)
                arr.values(2).isInstanceOf[Identifier] shouldBe true
              case _ =>
                fail("Expected ArrayConstructor for VALUES row")
          case _ =>
            fail(s"Expected Values, got ${child.getClass.getSimpleName}")
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
      case InsertInto(target, columns, child, _, _) =>
        getTableName(target) shouldBe "summary_table"
        columns shouldBe Nil
        // Verify the child is a complex query plan
        // The exact structure depends on the AST representation
        child.isInstanceOf[Relation] shouldBe true
        debug(s"Child plan type: ${child.getClass.getSimpleName}")
        // At minimum, verify it's not a simple Values node
        child.isInstanceOf[Values] shouldBe false
      case _ =>
        fail(s"Expected InsertInto, got ${plan.getClass.getSimpleName}")
  }

end InsertIntoParserTest
