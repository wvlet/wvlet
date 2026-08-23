/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL

/**
  * Standard library functions define `in hive` variants where Hive SQL differs from the other
  * engines. Hive has no execution backend in this repository, so these tests verify the generated
  * SQL for the Hive compile target, and that the DuckDB output is unaffected by the Hive variants.
  */
class HiveDialectResolutionTest extends UniTest:

  private def generateSQL(wv: String, dbType: DBType = DBType.Hive): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  test("select Hive string function variants") {
    val query =
      """from [['abc']] as t(s)
        |select
        |  s.regexp_like('a.*') as rl,
        |  s.contains('b') as c,
        |  s.starts_with('a') as sw,
        |  s.ends_with('c') as ew,
        |  s.strpos('b') as sp,
        |  s.md5 as m,
        |  s.sha256 as sh,
        |  s.json_extract_string('$.a') as je,
        |""".stripMargin
    val sql = generateSQL(query)
    sql shouldContain "s rlike 'a.*'"
    sql shouldContain "instr(s,'b') > 0"
    sql shouldContain "instr(s,'a') = 1"
    sql shouldContain "substr(s,-length('c')) = 'c'"
    sql shouldContain "instr(s,'b')"
    sql shouldContain "md5(s)"
    sql shouldContain "sha2(s,256)"
    sql shouldContain "get_json_object(s,'$.a')"
  }

  test("select Hive date and timestamp function variants") {
    val query =
      """from [['2024-03-15']] as t(ds)
        |select ds.to_date as d, ds.to_timestamp as tt
        |select
        |  d.add_days(1) as ad,
        |  d.add_months(2) as am,
        |  d.diff_days('2024-03-25'.to_date) as dd,
        |  d.day_of_week as dow,
        |  d.format('yyyy-MM-dd') as f,
        |  tt.to_unixtime as ut,
        |""".stripMargin
    val sql = generateSQL(query)
    sql shouldContain "date_add(d,1)"
    sql shouldContain "add_months(d,2)"
    sql shouldContain "datediff(cast('2024-03-25' as date),d)"
    sql shouldContain "pmod(dayofweek(d) + 5, 7) + 1"
    sql shouldContain "date_format(d,'yyyy-MM-dd')"
    sql shouldContain "cast(unix_timestamp(tt) as bigint)"
  }

  test("select Hive array and aggregation variants") {
    val query =
      """from [[1, 'x']] as t(id, s)
        |select id, s, [3, 1, 2] as a
        |select
        |  a.size as sz,
        |  a.get(1) as first,
        |  a.contains(2) as c,
        |  a.sort.mk_string(',') as st,
        |""".stripMargin
    val sql = generateSQL(query)
    sql shouldContain "size(a)"
    sql shouldContain "a[1 - 1]"
    sql shouldContain "array_contains(a,2)"
    sql shouldContain "concat_ws(',',sort_array(a))"
  }

  test("select Hive grouped aggregation variants") {
    val query =
      """from [[1, 'x', 1.0]] as t(id, s, vv)
        |select id, s, vv.to_double as v
        |group by id
        |agg s.string_agg('-') as sa, v.median as med
        |""".stripMargin
    val sql = generateSQL(query)
    sql shouldContain "concat_ws('-',collect_list(s))"
    sql shouldContain "percentile_approx(v, 0.5)"
  }

  test("cast to string instead of varchar for Hive") {
    val query =
      """from [[1]] as t(id)
        |select id.to_string as s
        |""".stripMargin
    generateSQL(query) shouldContain "cast(id as string)"
    generateSQL(query, DBType.DuckDB) shouldContain "cast(id as varchar)"
  }

  test("keep DuckDB output unaffected by the Hive variants") {
    val query =
      """from [['abc']] as t(s)
        |select s.contains('b') as c, s.md5 as m
        |""".stripMargin
    val sql = generateSQL(query, DBType.DuckDB)
    sql shouldContain "contains(s,'b')"
    sql shouldContain "md5(s)"
  }

end HiveDialectResolutionTest
