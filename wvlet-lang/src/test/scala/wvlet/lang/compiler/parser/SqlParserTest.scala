package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

trait SqlParserSpec(specPath: String, ignoredSpec: Map[String, String] = Map.empty) extends AirSpec:
  for unit <- CompilationUnit.fromPath(specPath) do
    // Rename spec path / to : for test name
    test(unit.sourceFile.relativeFilePath.replaceAll("/", ":")) {
      ignoredSpec.get(unit.sourceFile.fileName).foreach(reason => ignore(reason))
      val parser = SqlParser(unit, isContextUnit = true)
      val stmt   = parser.parse()
      debug(stmt.pp)
    }

class SqlParserBasicSpec extends SqlParserSpec("spec/sql/basic")
class SqlParserTPCHSpec  extends SqlParserSpec("spec/sql/tpc-h")
class SqlParserTPCDSSpec extends SqlParserSpec("spec/sql/tpc-ds")
class SqlParserHiveSpec
    extends SqlParserSpec(
      "spec/sql/hive",
      Map(
        "hive-data-types.sql" -> "Temporarily ignored - complex Hive data types not yet supported"
      )
    )

class SqlParserUpdateSpec extends SqlParserSpec("spec/sql/update")
class SqlParserTrinoSpec  extends SqlParserSpec("spec/sql/trino")

class SqlParserCommentTest extends AirSpec:
  test("should parse CREATE TABLE with COMMENT syntax") {
    val sql    = "CREATE TABLE test ( id bigint COMMENT 'test comment' );"
    val unit   = CompilationUnit.fromSqlString(sql)
    val parser = SqlParser(unit, isContextUnit = true)
    val stmt   = parser.parse()
    debug(stmt.pp)
  }

  test("should parse CREATE TABLE with COMMENT and WITH properties") {
    val sql  = "CREATE TABLE test ( f_9d304 bigint COMMENT 'Unixtime' WITH ( key_name = 'time' ) );"
    val unit = CompilationUnit.fromSqlString(sql)
    val parser = SqlParser(unit, isContextUnit = true)
    val stmt   = parser.parse()
    debug(stmt.pp)
  }
