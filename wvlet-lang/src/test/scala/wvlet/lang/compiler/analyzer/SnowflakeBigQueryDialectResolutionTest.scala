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
  * Standard library functions define `in snowflake` and `in bigquery` variants where those engines
  * differ from the others. Neither engine has an execution backend in this repository, so these
  * tests verify the generated SQL for each compile target, and that the DuckDB output is unaffected
  * by the added variants.
  */
class SnowflakeBigQueryDialectResolutionTest extends UniTest:

  private def generateSQL(wv: String, dbType: DBType): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private val stringQuery =
    """from [['abc']] as t(s)
      |select
      |  s.regexp_like('a.*') as rl,
      |  s.contains('b') as c,
      |  s.starts_with('a') as sw,
      |  s.ends_with('c') as ew,
      |  s.md5 as m,
      |  s.sha256 as sh,
      |  s.levenshtein('abd') as lv,
      |  s.json_extract_string('$.a') as je,
      |""".stripMargin

  test("select Snowflake string function variants") {
    val sql = generateSQL(stringQuery, DBType.Snowflake)
    sql shouldContain "regexp_instr(s,'a.*') > 0"
    sql shouldContain "contains(s,'b')"
    sql shouldContain "startswith(s,'a')"
    sql shouldContain "endswith(s,'c')"
    sql shouldContain "md5(s)"
    sql shouldContain "sha2(s,256)"
    sql shouldContain "editdistance(s,'abd')"
    sql shouldContain "json_extract_path_text(s,'$.a')"
  }

  test("select BigQuery string function variants") {
    val sql = generateSQL(stringQuery, DBType.BigQuery)
    sql shouldContain "regexp_contains(s,'a.*')"
    sql shouldContain "strpos(s,'b') > 0"
    sql shouldContain "starts_with(s,'a')"
    sql shouldContain "ends_with(s,'c')"
    sql shouldContain "lower(to_hex(md5(s)))"
    sql shouldContain "lower(to_hex(sha256(s)))"
    sql shouldContain "edit_distance(s,'abd')"
    sql shouldContain "json_extract_scalar(s,'$.a')"
  }

  private val dateQuery =
    """from [['2024-03-15']] as t(ds)
      |select ds.to_date as d, ds.to_timestamp as tt
      |select
      |  d.add_days(1) as ad,
      |  d.diff_days('2024-03-25'.to_date) as dd,
      |  d.day_of_week as dow,
      |  tt.add_hours(3) as ah,
      |  tt.to_unixtime as ut,
      |""".stripMargin

  test("select Snowflake date and timestamp function variants") {
    val sql = generateSQL(dateQuery, DBType.Snowflake)
    sql shouldContain "dateadd('day',1,d)"
    sql shouldContain "datediff('day',d,cast('2024-03-25' as date))"
    sql shouldContain "dayofweekiso(d)"
    sql shouldContain "dateadd('hour',3,tt)"
    sql shouldContain "cast(date_part('epoch_second',tt) as bigint)"
  }

  test("select BigQuery date and timestamp function variants") {
    val sql = generateSQL(dateQuery, DBType.BigQuery)
    sql shouldContain "date_add(d, interval (1) day)"
    sql shouldContain "date_diff(cast('2024-03-25' as date),d,day)"
    sql shouldContain "mod(extract(dayofweek from d) + 5, 7) + 1"
    sql shouldContain "timestamp_add(tt, interval (3) hour)"
    sql shouldContain "unix_seconds(tt)"
  }

  private val arrayQuery =
    """from [[1]] as t(id)
      |select id, [3, 1, 2] as a
      |select
      |  a.size as sz,
      |  a.get(1) as first,
      |  a.contains(2) as c,
      |  a.mk_string(',') as st,
      |""".stripMargin

  test("select Snowflake array function variants") {
    val sql = generateSQL(arrayQuery, DBType.Snowflake)
    sql shouldContain "array_size(a)"
    sql shouldContain "get(a,1 - 1)"
    sql shouldContain "array_contains(to_variant(2),a)"
    sql shouldContain "array_to_string(a,',')"
  }

  test("select BigQuery array function variants") {
    val sql = generateSQL(arrayQuery, DBType.BigQuery)
    sql shouldContain "array_length(a)"
    sql shouldContain "a[ordinal(1)]"
    sql shouldContain "2 in unnest(a)"
    sql shouldContain "array_to_string(a,',')"
  }

  private val aggQuery =
    """from [[1, 'x', 1.0]] as t(id, s, vv)
      |select id, s, vv.to_double as v
      |group by id
      |agg s.string_agg('-') as sa, v.median as med
      |""".stripMargin

  test("select Snowflake aggregation variants") {
    val sql = generateSQL(aggQuery, DBType.Snowflake)
    sql shouldContain "listagg(s,'-')"
    sql shouldContain "median(v)"
  }

  test("select BigQuery aggregation variants") {
    val sql = generateSQL(aggQuery, DBType.BigQuery)
    sql shouldContain "string_agg(s,'-')"
    sql shouldContain "approx_quantiles(v, 2)[offset(1)]"
  }

  test("use BigQuery cast type names") {
    val query =
      """from [['1']] as t(s)
        |select s.to_long as l, s.to_double as d, 1.to_string as ss
        |""".stripMargin
    val sql = generateSQL(query, DBType.BigQuery)
    sql shouldContain "cast(s as int64)"
    sql shouldContain "cast(s as float64)"
    sql shouldContain "cast(1 as string)"
  }

  test("keep DuckDB output unaffected by the new variants") {
    val sql = generateSQL(stringQuery, DBType.DuckDB)
    sql shouldContain "contains(s,'b')"
    sql shouldContain "md5(s)"
    sql shouldContain "starts_with(s,'a')"
  }

end SnowflakeBigQueryDialectResolutionTest
