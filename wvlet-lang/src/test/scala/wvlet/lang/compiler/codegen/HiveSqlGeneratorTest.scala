package wvlet.lang.compiler.codegen

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, DBType}
import wvlet.lang.compiler.parser.{SqlParser, WvletParser}

class HiveSqlGeneratorTest extends AirSpec:

  private def generateSQL(wvlet: String): String =
    val unit = CompilationUnit.fromWvletString(wvlet)
    val plan = WvletParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive))
    generator.print(plan)

  test("generate Hive-specific functions") {
    test("array_agg -> collect_list") {
      val sql = generateSQL("from t select x.array_agg as arr")
      debug(s"Generated SQL: $sql")
      sql.contains("collect_list(x)") shouldBe true
      sql.contains("array_agg") shouldBe false
    }

    test("regexp_like -> regexp") {
      val sql = generateSQL("from t where s.regexp_like('pattern')")
      debug(s"Generated SQL: $sql")
      sql.contains("regexp(s, 'pattern')") shouldBe true
      sql.contains("regexp_like") shouldBe false
    }
  }

  test("array constructor syntax") {
    val sql = generateSQL("select [1, 2, 3] as arr")
    // Hive uses ARRAY prefix syntax
    sql.contains("ARRAY[1, 2, 3]") shouldBe true
  }

  test("map constructor syntax") {
    val sql = generateSQL("select {a: 1, b: 2} as m")
    debug(s"Generated SQL: $sql")
    // Hive uses MAP(keys, values) syntax
    sql.contains("MAP(ARRAY['a', 'b'], ARRAY[1, 2])") shouldBe true
  }

  test("LATERAL VIEW for unnest") {
    pending("Need to add HiveRewriteUnnest to compiler phases")
    val sql = generateSQL("""
      from t 
      cross join unnest(arr) as u(elem)
      select elem
    """)
    sql.contains("LATERAL VIEW explode(arr)") shouldBe true
    sql.contains("unnest") shouldBe false
  }

  test("VALUES without parentheses") {
    val unit = CompilationUnit.fromSqlString("insert into t values (1, 'a'), (2, 'b')")
    val plan = SqlParser(unit).parse()
    val sql = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.Hive)).print(plan)
    debug(s"Generated SQL: $sql")
    // Hive doesn't require parentheses around VALUES
    sql.contains("values") shouldBe true
    sql.contains("(values") shouldBe false
  }