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
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
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
    result.reportAllErrors
    GenSQL.generateSQL(unit)(using result.context)

  private def compileError(wv: String, dbType: DBType = DBType.Wvlet): WvletLangException =
    intercept[WvletLangException] {
      generateSQL(wv, dbType)
    }

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

  test("the wvlet engine should use the duckdb variants of the standard library") {
    val query =
      """from [['abc']] as t(s)
        |select s.regexp_like('a.*') as m
        |""".stripMargin
    generateSQL(query, DBType.Wvlet) shouldContain "regexp_matches(s,'a.*')"
  }

  test("an in wvlet variant should win over an in duckdb variant on the wvlet engine") {
    val query =
      """def f_engine(x: int) in duckdb: int = x + 1
        |def f_engine(x: int) in wvlet: int = x + 2
        |
        |from [[10]] as t(v)
        |select f_engine(v) as r
        |""".stripMargin
    generateSQL(query, DBType.Wvlet) shouldContain "v + 2"
    generateSQL(query, DBType.DuckDB) shouldContain "v + 1"
  }

  test("the wvlet engine should accept the functions it binds") {
    val query =
      """from [[0.5]] as t(v)
        |select acos(v) as a, v.acos() as b
        |""".stripMargin
    generateSQL(query, DBType.Wvlet) shouldContain "acos(v)"
  }

  test("a DuckDB-only function should fail at compile time on the wvlet engine") {
    val query =
      """from [[[1, 2]]] as t(a)
        |select array_unique(a) as u
        |""".stripMargin
    val e = compileError(query)
    e.statusCode shouldBe StatusCode.FUNCTION_NOT_FOUND
    e.getMessage shouldContain "array_unique is not supported by the wvlet engine"
    e.getMessage shouldContain "DuckDB connector"
    // The DuckDB connector keeps the full DuckDB catalog
    generateSQL(query, DBType.DuckDB) shouldContain "array_unique(a)"
  }

  test("a method-style call of a DuckDB-only function should fail on the wvlet engine") {
    val query =
      """from [[[1, 2]]] as t(a)
        |select a.array_unique() as u
        |""".stripMargin
    compileError(query).statusCode shouldBe StatusCode.FUNCTION_NOT_FOUND
  }

  test("a function defined only for another engine should fail on the wvlet engine") {
    val query =
      """def f_trino_only(x: int) in trino: int = x + 1
        |
        |from [[10]] as t(v)
        |select f_trino_only(v) as r
        |""".stripMargin
    compileError(query).getMessage shouldContain "f_trino_only is not supported"
    // Other engines keep the lenient fallback to the only definition
    generateSQL(query, DBType.DuckDB) shouldContain "v + 1"
  }

  test("the error should name the stdlib function that expands to a DuckDB name") {
    EngineFunctionValidator.stdlibAlternative("regexp_matches") shouldBe Some("string.regexp_like")
  }

end DialectFunctionResolutionTest
