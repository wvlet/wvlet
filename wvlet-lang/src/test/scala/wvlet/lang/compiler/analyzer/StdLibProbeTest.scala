package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL

/** Standard library methods must resolve for each primitive column type */
class StdLibProbeTest extends UniTest:

  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    if result.hasFailures then
      result
        .failureReport
        .foreach { case (u, e) =>
          info(s"Failure in ${u.sourceFile.fileName}: ${e}")
        }
    GenSQL.generateSQL(unit)(using result.context)

  test("resolve methods on a negative integer column") {
    val sql = generateSQL("from [[-5]] as t(i)\nselect i.abs as a\n")
    sql shouldContain "abs(i)"
  }

  test("resolve methods on a decimal column") {
    val sql = generateSQL("from [[1.75]] as t(d)\nselect d.floor as f, d.truncate as tr\n")
    sql shouldContain "floor(d)"
    sql shouldContain "trunc(d)"
  }

  test("resolve dialect-scoped defs through a method chain") {
    val sql = generateSQL("from [[1.75]] as t(d)\nselect d.to_double.is_finite as fi\n")
    sql shouldContain "isfinite(cast(d as double))"
  }

  test("resolve methods on a double literal") {
    val sql = generateSQL("select 100.0.log10 as l\n")
    sql shouldContain "log10(100.0)"
  }

  test("probe map literal typing") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = DBType.DuckDB))
    val unit     = CompilationUnit.fromWvletString(
      "select map {\"a\": 1} as m\nselect m.keys as ks, m.get('a') as v\n"
    )
    val result = compiler.compileSingleUnit(unit)
    info(unit.resolvedPlan.pp)
    val sql = GenSQL.generateSQL(unit)(using result.context)
    info(sql)
    sql shouldContain "map_keys(m)"
  }

  test("probe chained array methods") {
    val sql = generateSQL("select [3, 1, 2].distinct.sort as s\n")
    info(sql)
    sql shouldContain "list_sort(array_distinct("
  }

  test("probe a member call with args chained on an inlined array member") {
    val sql = generateSQL("from [[1]] as t(i)\nselect [3, 1, 2].sort.mk_string(',') as s\n")
    info(sql)
    sql shouldContain "array_to_string(list_sort("
  }

  test("probe a member call with args chained on an arg-taking member") {
    val sql = generateSQL("from [[1]] as t(i)\nselect [3, 1, 2].concat([9]).mk_string(',') as s\n")
    info(sql)
    sql shouldContain "array_to_string(list_concat("
  }

  test("probe a member call with args chained on map keys") {
    val sql = generateSQL("select map {\"a\": 1} as m\nselect m.keys.mk_string(',') as ks\n")
    info(sql)
    sql shouldContain "array_to_string(map_keys(m)"
  }

end StdLibProbeTest
