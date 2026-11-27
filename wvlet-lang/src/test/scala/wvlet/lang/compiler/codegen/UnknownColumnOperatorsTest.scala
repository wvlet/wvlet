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
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.test.UniTest

/**
  * Column operators over a table with no known schema keep the table's other columns through star
  * modifiers, or report that the columns are unknown when the SQL dialect has none
  */
class UnknownColumnOperatorsTest extends UniTest:
  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private def normalized(sql: String): String = sql
    .linesIterator
    .filterNot(_.startsWith("--"))
    .map(_.trim)
    .mkString(" ")

  private def sql(wv: String, dbType: DBType = DBType.DuckDB): String = normalized(
    generateSQL(wv, dbType)
  )

  test("exclude, rename, and shift keep the other columns of a table with no schema") {
    sql("from lineitem exclude l_comment") shouldBe "select * exclude (l_comment) from lineitem"
    sql("from lineitem rename l_shipdate as ship_date") shouldBe
      "select * rename (l_shipdate as ship_date) from lineitem"
    sql("from lineitem shift l_orderkey") shouldBe
      "select l_orderkey, * exclude (l_orderkey) from lineitem"
    sql("from lineitem shift to right l_orderkey") shouldBe
      "select * exclude (l_orderkey), l_orderkey from lineitem"
  }

  test("keep the unknown columns after add") {
    sql("from lineitem add 1 as r exclude l_comment") shouldBe
      "select * exclude (l_comment) from ( select *, 1 as r from lineitem )"
    sql("from lineitem add to left 1 as r exclude l_comment") shouldBe
      "select * exclude (l_comment) from ( select 1 as r, * from lineitem )"
  }

  test("compile the introduction's chained column operators") {
    sql("""from lineitem
        |add l_quantity * l_extendedprice as revenue
        |rename l_shipdate as ship_date
        |exclude l_comment
        |shift l_orderkey, revenue, ship_date
        |limit 5""".stripMargin) shouldBe
      "select l_orderkey, revenue, ship_date, * exclude (l_orderkey, revenue, ship_date) " +
      "from ( select * exclude (l_comment) " + "from ( select * rename (l_shipdate as ship_date) " +
      "from ( select *, l_quantity * l_extendedprice as revenue from lineitem ) ) ) limit 5"
  }

  test("use explicit columns after a select closes the column list") {
    sql("from lineitem select l_orderkey, l_quantity exclude l_quantity") shouldNotContain
      "* exclude"
  }

  test("keep explicit column lists for a known schema") {
    sql("from [[1, 2, 3]] as t(a, b, c) exclude b") shouldNotContain "* exclude"
    sql("from [[1, 2, 3]] as t(a, b, c) exclude b") shouldContain "select a, c"
  }

  test("use each dialect's star modifier") {
    sql("from lineitem exclude l_comment", DBType.Snowflake) shouldContain
      "select * exclude (l_comment)"
    sql("from lineitem rename l_shipdate as ship_date", DBType.Snowflake) shouldContain
      "select * rename (l_shipdate as ship_date)"
    sql("from lineitem exclude l_comment", DBType.BigQuery) shouldContain
      "select * except (l_comment)"
    sql("from lineitem shift l_orderkey", DBType.BigQuery) shouldContain
      "select l_orderkey, * except (l_orderkey)"
  }

  test("report unknown columns when the dialect has no star modifier") {
    val e = intercept[WvletLangException] {
      generateSQL("from lineitem exclude l_comment", DBType.Trino)
    }
    e.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
    e.getMessage shouldContain "exclude needs the columns of lineitem"
    e.getMessage shouldContain "* exclude"

    val r = intercept[WvletLangException] {
      generateSQL("from lineitem rename l_shipdate as ship_date", DBType.BigQuery)
    }
    r.getMessage shouldContain "rename needs the columns of lineitem"

    intercept[WvletLangException] {
      generateSQL("from lineitem shift l_orderkey", DBType.Generic)
    }
  }

end UnknownColumnOperatorsTest
