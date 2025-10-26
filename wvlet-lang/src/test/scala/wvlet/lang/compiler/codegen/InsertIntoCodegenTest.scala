package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.SqlParser

class InsertIntoCodegenTest extends AirSpec:

  private def generateSQL(sql: String, dbType: DBType = DBType.Generic): String =
    val unit      = CompilationUnit.fromSqlString(sql)
    val plan      = SqlParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = dbType))
    generator.print(plan)

  test("generate INSERT INTO for different DB types") {
    val sql = "INSERT INTO users VALUES (1, 'Alice', 25)"

    // Test different DB types and their expected VALUES syntax
    val testCases = Seq(
      (DBType.Generic, "values (1, 'Alice', 25)"),
      (DBType.Trino, "(values (1, 'Alice', 25))"),
      (DBType.DuckDB, "values (1, 'Alice', 25)")
    )

    testCases.foreach { case (dbType, expectedValuesClause) =>
      val generatedSQL = generateSQL(sql, dbType)
      debug(s"$dbType: $generatedSQL")
      generatedSQL shouldBe s"insert into users \n$expectedValuesClause"
    }
  }

  test("generate INSERT INTO with column list") {
    val sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"

    Seq(DBType.Generic, DBType.Trino).foreach { dbType =>
      val generatedSQL = generateSQL(sql, dbType)
      debug(s"$dbType: $generatedSQL")
      val expectedPrefix = "insert into users (id, name, age) \n"
      generatedSQL.startsWith(expectedPrefix) shouldBe true

      // Verify the VALUES clause format
      if dbType == DBType.Trino then
        generatedSQL shouldContain "(values (1, 'Alice', 25))"
      else
        generatedSQL shouldContain "values (1, 'Alice', 25)"
    }
  }

  test("generate INSERT INTO with multi-row VALUES") {
    val sql = "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)"

    val genericSQL = generateSQL(sql, DBType.Generic)
    debug(s"Generic multi-row: $genericSQL")

    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(s"Trino multi-row: $trinoSQL")
    // Trino should have (values ...)
    trinoSQL shouldContain "(values"
  }

  test("generate INSERT INTO with SELECT") {
    val sql = "INSERT INTO users SELECT * FROM temp_users"

    Seq(DBType.Generic, DBType.Trino).foreach { dbType =>
      val generatedSQL = generateSQL(sql, dbType)
      debug(s"$dbType SELECT: $generatedSQL")

      // Verify exact structure
      generatedSQL shouldBe "insert into users \nselect * from temp_users"
    }
  }

  test("generate INSERT INTO with qualified table names") {
    val sql = """INSERT INTO "catalog"."schema".users VALUES (1, 'Alice', 25)"""

    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(trinoSQL)
    // catalog and schema are SQL keywords, so they should be quoted
    trinoSQL shouldContain """insert into "catalog"."schema".users"""
  }

end InsertIntoCodegenTest
