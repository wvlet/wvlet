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
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.model.plan.Limit
import wvlet.lang.model.plan.Offset
import wvlet.uni.test.UniTest

/**
  * Generated SQL for pipelines that SqlGenerator must not fold into one SELECT block. The Lean
  * model in formal/WvletFormal/SqlFold proves the folding rules; SqlFoldConformanceTest replays
  * them. These cases pin the SQL text of the folds that used to change results.
  */
class SqlBlockFoldingTest extends UniTest:

  private def config = CodeFormatterConfig(sqlDBType = DBType.DuckDB)

  private def wvletToSQL(wv: String): String =
    val plan = WvletParser(CompilationUnit.fromWvletString(wv)).parse()
    SqlGenerator(config).print(plan)

  private def sqlToSQL(sql: String): String =
    val plan = SqlParser(CompilationUnit.fromSqlString(sql)).parse()
    SqlGenerator(config).print(plan)

  private def selects(sql: String): Int = "(?i)\\bselect\\b".r.findAllIn(sql).size

  test("nest a filter after a limit") {
    val sql = wvletToSQL("from t limit 3 where x > 0")
    selects(sql) shouldBe 2
    // The filter applies to the limited rows, outside the subquery
    sql.indexOf("limit 3") < sql.indexOf("where x > 0") shouldBe true
  }

  test("nest a group by and a select distinct after a limit") {
    selects(wvletToSQL("from t limit 3 group by k")) shouldBe 2
    selects(wvletToSQL("from t limit 3 select distinct a")) shouldBe 2
    selects(wvletToSQL("from t limit 3 agg x.sum as s")) shouldBe 2
  }

  test("keep a limit after a count outside the count") {
    val sql = wvletToSQL("from t count limit 0")
    selects(sql) shouldBe 1
    sql shouldContain "count(*)"
    sql shouldContain "limit 0"
  }

  test("keep DISTINCT on its own projection") {
    val sql = wvletToSQL("from t select distinct a select b")
    selects(sql) shouldBe 2
    sql shouldContain "select distinct a"
    sql shouldNotContain "select distinct b"
  }

  test("fold a projection under a later dedup into select distinct") {
    wvletToSQL("from t select a dedup") shouldContain "select distinct a"
    val sql = wvletToSQL("from t dedup select a")
    selects(sql) shouldBe 2
    sql shouldNotContain "select distinct a"
  }

  test("nest a filter that reads a computed or renamed column") {
    selects(wvletToSQL("from t select a as b where b > 0")) shouldBe 2
    selects(wvletToSQL("from t select a, x + 1 as c where c > 0")) shouldBe 2
    selects(wvletToSQL("from t group by k select k, _.count as c where c > 1")) shouldBe 2
  }

  test("nest a filter on the field of a projected struct") {
    selects(wvletToSQL("from t select s.y where y > 0")) shouldBe 2
  }

  test("parenthesize a nested group by under a refused HAVING") {
    val sql = wvletToSQL("from t group by a where a > 0 count")
    selects(sql) shouldBe 2
    sql shouldContain "from ("
    SqlParser(CompilationUnit.fromSqlString(sql)).parse()
  }

  test("keep a filter on a passed-through column in the same block") {
    selects(wvletToSQL("from t select a, x + 1 as c where a > 0")) shouldBe 1
    selects(wvletToSQL("from t where a > 0 select a")) shouldBe 1
  }

  test("nest an order by under a group by or a select distinct") {
    selects(wvletToSQL("from t order by x group by k")) shouldBe 2
    selects(wvletToSQL("from t order by x select distinct a")) shouldBe 2
    // ORDER BY after the projection stays in the block
    selects(wvletToSQL("from t select a order by a")) shouldBe 1
  }

  test("keep a distinct aggregation with its group by") {
    val sql = wvletToSQL("from t group by k select distinct k, _.count as c where k > 0")
    selects(sql) shouldBe 2
    // The aggregation stays in the inner block with its GROUP BY
    sql.indexOf("group by k") < sql.indexOf("where k > 0") shouldBe true
  }

  test("render the select list of a nested group by") {
    val sql = wvletToSQL("from t group by k group by k")
    selects(sql) shouldBe 2
    sql shouldNotContain "select *"
  }

  test("keep ORDER BY on an unselected input column of a SQL query flat") {
    selects(sqlToSQL("select a as b from t order by a")) shouldBe 1
  }

  test("parse LIMIT and OFFSET into a limit over an offset") {
    val plan  = SqlParser(CompilationUnit.fromSqlString("select * from t limit 3 offset 2")).parse()
    var found = false
    plan.traverse { case Limit(_: Offset, _, _) =>
      found = true
    }
    found shouldBe true
  }

  test("render LIMIT and OFFSET in one block") {
    val sql = sqlToSQL("select * from t order by x limit 3 offset 2")
    selects(sql) shouldBe 1
    sql shouldContain "limit 3"
    sql shouldContain "offset 2"
  }

  test("nest an offset after a limit") {
    // `(… limit 3) offset 2` takes three rows, then skips two
    selects(sqlToSQL("select * from (select * from t order by x limit 3) offset 2")) shouldBe 2
  }

end SqlBlockFoldingTest
