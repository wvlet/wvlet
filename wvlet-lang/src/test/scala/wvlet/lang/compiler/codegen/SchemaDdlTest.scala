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
package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.parser.WvletParser

/**
  * Schema DDL refinements (#1995): `create schema s in '<uri>'` locations and `with k: v` options
  * render as Trino schema properties (rejected loudly on engines without them), and
  * `drop schema s with cascade: true` renders as a CASCADE drop
  */
class SchemaDdlTest extends UniTest:

  private def generateSQL(wvlet: String, dbType: DBType = DBType.Trino): String =
    val unit = CompilationUnit.fromWvletString(wvlet)
    val plan = WvletParser(unit).parse()
    SqlGenerator(CodeFormatterConfig(sqlDBType = dbType)).print(plan)

  test("should render a schema location as a Trino location property") {
    val sql = generateSQL("create schema sales in 's3://bucket/sales/' if not exists")
    sql shouldContain "create schema if not exists sales"
    sql shouldContain "with (location = 's3://bucket/sales/')"
  }

  test("should render schema options as properties alongside the location") {
    val sql = generateSQL("create schema sales in 's3://b/' with owner: 'etl'")
    sql shouldContain "with (location = 's3://b/', owner = 'etl')"
  }

  test("should render drop schema cascade") {
    val sql = generateSQL("drop schema staging if exists with cascade: true")
    sql shouldContain "drop schema if exists staging cascade"
  }

  test("should omit cascade when false") {
    val sql = generateSQL("drop schema staging with cascade: false")
    sql shouldContain "drop schema staging"
    sql.contains("cascade") shouldBe false
  }

  test("should reject unknown drop schema options") {
    val e = intercept[WvletLangException] {
      generateSQL("drop schema s with force: true")
    }
    e.getMessage shouldContain "Unknown drop schema option 'force'"
  }

  test("should reject non-boolean cascade values") {
    val e = intercept[WvletLangException] {
      generateSQL("drop schema s with cascade: 'yes'")
    }
    e.getMessage shouldContain "cascade expects a boolean value"
  }

  test("should reject schema locations on engines without schema properties") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = DBType.DuckDB))
    val unit     = CompilationUnit.fromWvletString("create schema sales in 's3://bucket/sales/'")
    val result   = compiler.compileSingleUnit(unit)
    val e        = intercept[WvletLangException] {
      GenSQL.generateSQL(unit)(using result.context)
    }
    e.getMessage shouldContain "with a location or options is not supported on"
  }

end SchemaDdlTest
