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
  * Standard library functions can define multiple variants of the same function scoped to different
  * database dialects (e.g. `def regexp_like in duckdb` and `def regexp_like in trino`). These tests
  * verify that the variant matching the compile target is selected, with fallback to the
  * dialect-neutral definition, and that overloads with different arities resolve by argument count.
  */
class DialectFunctionResolutionTest extends UniTest:

  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  test("select the dialect-specific variant matching the compile target") {
    val query =
      """from [['abc']] as t(s)
        |select s.regexp_like('a.*') as m
        |""".stripMargin
    generateSQL(query, DBType.DuckDB) shouldContain "regexp_matches(s,'a.*')"
    generateSQL(query, DBType.Trino) shouldContain "regexp_like(s,'a.*')"
  }

  test("select the dialect-specific aggregation variant matching the compile target") {
    val query =
      """from [[1, 'a'], [1, 'b']] as t(id, name)
        |group by id
        |select id, name.count_approx_distinct as ndv
        |""".stripMargin
    generateSQL(query, DBType.DuckDB) shouldContain "approx_count_distinct(name)"
    generateSQL(query, DBType.Trino) shouldContain "approx_distinct(name)"
  }

  test("fall back to the dialect-neutral variant when no dialect-specific one matches") {
    val query =
      """def f_neutral(x: int): int = x + 1
        |def f_neutral(x: int) in trino: int = x + 2
        |
        |from [[10]] as t(v)
        |select f_neutral(v) as r
        |""".stripMargin
    generateSQL(query, DBType.DuckDB) shouldContain "v + 1"
    generateSQL(query, DBType.Trino) shouldContain "v + 2"
  }

  test("pass through engine functions from the bundled DuckDB catalog") {
    // bit_count is not part of the hand-written stdlib; it comes from the generated
    // module/standard/duckdb/functions.wv catalog and must compile to a plain SQL call
    val query =
      """from [[5]] as t(i)
        |select bit_count(i) as b
        |""".stripMargin
    generateSQL(query, DBType.DuckDB) shouldContain "bit_count(i)"
  }

  test("resolve method overloads by argument count") {
    val query =
      """type string in duckdb = {
        |  def take_str(n: int): string = sql"substring(${this}, 1, ${n})"
        |  def take_str(n: int, pad: string): string = sql"rpad(substring(${this}, 1, ${n}), ${n}, ${pad})"
        |}
        |
        |from [['abc']] as t(s)
        |select s.take_str(2) as h1, s.take_str(5, '_') as h2
        |""".stripMargin
    val sql = generateSQL(query, DBType.DuckDB)
    sql shouldContain "substring(s, 1, 2)"
    sql shouldContain "rpad(substring(s, 1, 5), 5, '_')"
  }

end DialectFunctionResolutionTest
