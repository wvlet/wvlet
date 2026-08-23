package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.WvletParser

class AttachDatabaseSqlTest extends UniTest:

  private def generateSQL(wvlet: String): String =
    val unit = CompilationUnit.fromWvletString(wvlet)
    val plan = WvletParser(unit).parse()
    SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.DuckDB)).print(plan)

  test("should lower use-as to an attach statement") {
    val sql = generateSQL("use 'archive.duckdb' as archive")
    sql shouldContain "attach 'archive.duckdb' as archive"
  }

  test("should infer the engine type from the uri scheme") {
    val sql = generateSQL("use 'postgres://host/db' as pg")
    sql shouldContain "attach 'postgres://host/db' as pg"
    sql shouldContain "TYPE postgres"
  }

  test("should print boolean options as bare flags") {
    val sql = generateSQL("use 'x.duckdb' as x with read_only: true")
    sql shouldContain "attach 'x.duckdb' as x"
    sql shouldContain "READ_ONLY"
  }

  test("should let an explicit engine option override scheme inference") {
    val sql = generateSQL("use 'postgres://host/db' as pg with engine: 'mysql'")
    sql shouldContain "TYPE mysql"
    sql.contains("TYPE postgres") shouldBe false
  }

end AttachDatabaseSqlTest
