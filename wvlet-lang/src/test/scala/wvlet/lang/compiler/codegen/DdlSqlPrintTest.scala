package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.WvletParser

class DdlSqlPrintTest extends UniTest:

  private def generateSQL(wvlet: String): String =
    val unit = CompilationUnit.fromWvletString(wvlet)
    val plan = WvletParser(unit).parse()
    SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.DuckDB)).print(plan)

  test("should print rename schema as an alter schema statement") {
    val sql = generateSQL("rename schema staging to archive")
    sql shouldContain "alter schema staging rename to archive"
  }

  test("should print rename table as an alter table statement") {
    val sql = generateSQL("rename table users to customers")
    sql shouldContain "alter table users rename to customers"
  }

end DdlSqlPrintTest
