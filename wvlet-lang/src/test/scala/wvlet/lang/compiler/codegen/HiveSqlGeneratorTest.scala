package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.compiler.parser.WvletParser

class HiveSqlGeneratorTest extends AirSpec:

  private def generateSQL(wvlet: String): String =
    val unit      = CompilationUnit.fromWvletString(wvlet)
    val plan      = WvletParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive))
    generator.print(plan)

  test("generate Hive-specific functions") {
    // Note: Function transformation is now done in HiveRewriteFunctions phase
    // These tests would need full compilation pipeline to work properly
    pending("Function transformation requires full compilation pipeline")
  }

  test("array constructor syntax") {
    val sql = generateSQL("select [1, 2, 3] as arr")
    // Hive uses ARRAY prefix syntax
    sql shouldContain "ARRAY[1, 2, 3]"
  }

  test("struct syntax for Hive") {
    // In Hive, struct literals should use named_struct
    val sql = generateSQL("select {a: 1, b: 2} as s")
    debug(s"Generated SQL: $sql")
    // For now, Wvlet outputs struct syntax as-is
    // TODO: Transform to named_struct('a', 1, 'b', 2) for Hive
    sql shouldContain "{a: 1, b: 2}"
  }

  test("LATERAL VIEW for unnest") {
    // Note: This test only uses the parser, not the full compiler with transformations
    // The HiveRewriteUnnest transformation is already added to compiler phases,
    // but to test it properly we would need to run the full compilation pipeline
    pending("Test needs full compilation pipeline, not just parser")
    val sql = generateSQL("""
      from t 
      cross join unnest(arr) as u(elem)
      select elem
    """)
    sql shouldContain "LATERAL VIEW explode(arr)"
    sql shouldNotContain "unnest"
  }

  test("VALUES without parentheses") {
    val unit = CompilationUnit.fromSqlString("insert into t values (1, 'a'), (2, 'b')")
    val plan = SqlParser(unit).parse()
    val sql  = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive)).print(plan)
    debug(s"Generated SQL: $sql")
    // Hive doesn't require parentheses around VALUES
    sql shouldContain "values"
    sql shouldNotContain "(values"
  }

end HiveSqlGeneratorTest
