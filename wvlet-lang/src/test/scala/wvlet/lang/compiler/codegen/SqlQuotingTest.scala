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

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.WvletParser
import wvlet.uni.test.UniTest

/**
  * Quoting and escaping edge cases in generated SQL: identifiers with embedded double quotes
  * (#1697) and multi-line string literals (#1698)
  */
class SqlQuotingTest extends UniTest:

  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val unit      = CompilationUnit.fromWvletString(wv)
    val plan      = WvletParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = dbType))
    generator.print(plan)

  test("escape embedded double quotes in quoted identifiers") {
    val sql = generateSQL("select 1 as `my\"col`")
    sql shouldContain "\"my\"\"col\""
  }

  test("keep plain quoted identifiers unchanged") {
    val sql = generateSQL("select 1 as `group by`")
    sql shouldContain "\"group by\""
  }

  test("preserve newlines of triple-quoted strings via chr(10)") {
    val sql = generateSQL("select \"\"\"hello\nworld\"\"\" as greeting")
    sql shouldContain "'hello' || chr(10) || 'world'"
  }

  test("preserve trailing newlines of triple-quoted strings") {
    val sql = generateSQL("select \"\"\"hello\n\"\"\" as greeting")
    sql shouldContain "'hello' || chr(10) || ''"
  }

end SqlQuotingTest
