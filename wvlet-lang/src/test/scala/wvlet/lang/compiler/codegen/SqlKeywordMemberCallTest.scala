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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.parser.WvletParser
import wvlet.uni.test.UniTest

/**
  * Member calls whose qualifier type is unknown (no table schema declared) reach the SQL generator
  * un-inlined. Keyword-named ones (`like`, `in`, `between`, `extract`) must lower to their operator
  * form instead of the invalid `x."like"(...)` function call. Other member calls keep the plain
  * function-call form: a schema-qualified call such as `main.left(...)` has the same shape and is
  * valid SQL, so nothing is rejected at this level.
  */
class SqlKeywordMemberCallTest extends UniTest:

  // Parse-only: no analyzer runs, so column types stay unresolved as in a schema-less work folder
  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val unit      = CompilationUnit.fromWvletString(wv)
    val plan      = WvletParser(unit).parse()
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = dbType))
    generator.print(plan)

  test("lower like to the like operator") {
    val sql = generateSQL("from orders where o_comment.like('%special%requests%')")
    sql shouldContain "o_comment like '%special%requests%'"
    sql shouldNotContain "\"like\""
  }

  test("keep not in front of a lowered like") {
    val sql = generateSQL("from orders where !o_comment.like('%x%')")
    sql shouldContain "not o_comment like '%x%'"
  }

  test("lower in to the in operator with a value list") {
    val sql = generateSQL("from lineitem where l_shipmode.in('AIR', 'AIR REG')")
    sql shouldContain "l_shipmode in ('AIR', 'AIR REG')"
  }

  test("lower in with a single subquery argument") {
    val sql = generateSQL("from orders where o_orderkey.in(from lineitem select l_orderkey)")
    sql shouldContain "o_orderkey in ("
    sql shouldContain "select l_orderkey"
    sql shouldNotContain "\"in\""
  }

  test("lower not_in to the not in operator") {
    val sql = generateSQL("from partsupp where ps_suppkey.not_in(1, 2)")
    sql shouldContain "ps_suppkey not in (1, 2)"
  }

  test("lower between to the between operator") {
    val sql = generateSQL("from part where p_size.between(1, 5)")
    sql shouldContain "p_size between 1 and 5"
    sql shouldNotContain "\"between\""
  }

  test("lower extract to the extract function") {
    val sql = generateSQL("from lineitem select l_year = l_shipdate.extract('year')")
    // Same spelling as the Extract node, which prints `extract (FIELD from x)`
    sql shouldContain "extract (YEAR from l_shipdate)"
  }

  test("reject an unknown extract field") {
    val e = intercept[WvletLangException] {
      generateSQL("from lineitem select l_shipdate.extract('fortnight')")
    }
    e.statusCode shouldBe StatusCode.SYNTAX_ERROR
    e.getMessage shouldContain "fortnight"
  }

  test("reject a wrong argument count for between") {
    val e = intercept[WvletLangException] {
      generateSQL("from part where p_size.between(1)")
    }
    e.statusCode shouldBe StatusCode.SYNTAX_ERROR
  }

  test("reject in without arguments") {
    val e = intercept[WvletLangException] {
      generateSQL("from part where p_size.in()")
    }
    e.statusCode shouldBe StatusCode.SYNTAX_ERROR
  }

  test("match method names exactly like the typed resolution path") {
    // LIKE is not a stdlib method name, so it stays a plain call as with a typed qualifier
    val sql = generateSQL("from orders where o_comment.LIKE('%x%')")
    sql shouldNotContain "o_comment like"
  }

  test("keep schema-qualified keyword-named function calls untouched") {
    // A qualified function reference such as main.left(...) has the same shape as a member
    // call; engines accept it as main."left"(...), so it must not be rejected or lowered
    val sql = generateSQL("from t select main.left(s, 1)")
    sql shouldContain "main.\"left\"(s, 1)"
  }

  test("keep non-keyword member calls as function calls") {
    val sql = generateSQL("from part select p_name.upper()")
    sql shouldContain "p_name.upper()"
  }

end SqlKeywordMemberCallTest
