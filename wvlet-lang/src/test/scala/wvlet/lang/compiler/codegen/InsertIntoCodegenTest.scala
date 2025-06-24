package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, DBType}
import wvlet.lang.compiler.parser.SqlParser

class InsertIntoCodegenTest extends AirSpec:

  private def generateSQL(sql: String, dbType: DBType = DBType.Generic): String =
    val unit      = CompilationUnit.fromSqlString(sql)
    val plan      = SqlParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = dbType))
    generator.print(plan)

  test("generate INSERT INTO for different DB types") {
    val sql = "INSERT INTO users VALUES (1, 'Alice', 25)"

    // Generic SQL (no parentheses around VALUES)
    val genericSQL = generateSQL(sql, DBType.Generic)
    debug(s"Generic: $genericSQL")
    genericSQL shouldContain "insert into users"
    genericSQL shouldContain "values (1, 'Alice', 25)"

    // Trino (requires parentheses around VALUES)
    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(s"Trino: $trinoSQL")
    trinoSQL shouldContain "(values (1, 'Alice', 25))"

    // DuckDB (no parentheses around VALUES)
    val duckdbSQL = generateSQL(sql, DBType.DuckDB)
    debug(s"DuckDB: $duckdbSQL")
    duckdbSQL shouldContain "values (1, 'Alice', 25)"
  }

  test("generate INSERT INTO with column list") {
    val sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"

    val genericSQL = generateSQL(sql, DBType.Generic)
    debug(genericSQL)
    genericSQL shouldContain "insert into users (id, name, age)"

    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(trinoSQL)
    trinoSQL shouldContain "insert into users (id, name, age)"
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

    val genericSQL = generateSQL(sql, DBType.Generic)
    debug(s"Generic SELECT: $genericSQL")
    genericSQL.toLowerCase shouldContain "insert into users"
    genericSQL.toLowerCase shouldContain "select"

    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(s"Trino SELECT: $trinoSQL")
    trinoSQL.toLowerCase shouldContain "insert into users"
    trinoSQL.toLowerCase shouldContain "select"
  }

  test("generate INSERT INTO with qualified table names") {
    val sql = "INSERT INTO catalog.schema.users VALUES (1, 'Alice', 25)"

    val trinoSQL = generateSQL(sql, DBType.Trino)
    debug(trinoSQL)
    trinoSQL shouldContain "insert into catalog.schema.users"
  }

end InsertIntoCodegenTest
