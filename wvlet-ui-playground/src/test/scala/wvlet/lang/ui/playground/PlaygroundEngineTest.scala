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
package wvlet.lang.ui.playground

import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.uni.test.UniTest

/** Every bundled example compiles, translates to SQL, and runs on the engine */
class PlaygroundEngineTest extends UniTest:
  private val engine = PlaygroundEngine(0.001)

  SampleQuery
    .allFiles
    .foreach { (path, text) =>
      test(s"should run example ${path}") {
        val mode =
          if path.endsWith(".sql") then
            InputMode.SQL
          else
            InputMode.Wvlet
        val out = engine.run(text, mode)
        out.error shouldBe None
        out.sql.nonEmpty shouldBe true
        out.logicalPlan.nonEmpty shouldBe true
        out.result.isDefined shouldBe true
        if mode == InputMode.SQL then
          out.wvlet shouldContain "from"
      }
    }

  test("should run an example with vals again on the same engine") {
    // Runs share long-lived compilers; a previous run's vals must not hide the next run's
    val text = Examples.find("04_joins/06_asof_join.wv").get.text
    (1 to 3).foreach { _ =>
      engine.run(text).error shouldBe None
    }
    engine.run(text, position = LinePosition(16, 1)).error shouldBe None
  }

  test("should show SQL in other dialects") {
    val out = engine.run("from nation limit 3", dialect = SqlDialect.Trino)
    out.sql shouldContain "limit 3"
    out.result.get.rows.size shouldBe 3
  }

  test("should run only the statement at the cursor") {
    val text =
      """from nation limit 1
        |
        |from region
        |""".stripMargin
    val first = engine.run(text, position = LinePosition(1, 1))
    first.result.get.columns.head.name shouldBe "n_nationkey"
    val last = engine.run(text)
    last.result.get.columns.head.name shouldBe "r_regionkey"
  }

  test("should describe a query") {
    val out = engine.run(
      "from nation",
      position = LinePosition(1, 1),
      selection = QuerySelection.Describe
    )
    out.error shouldBe None
    out.result.get.rows.map(_.head.get) shouldContain "n_name"
  }

  test("should report a compile error") {
    val out = engine.run("from nation where")
    out.hasError shouldBe true
  }

  test("should convert SQL to Wvlet") {
    engine.toWvlet("select n_name from nation where n_regionkey = 1") shouldContain "from nation"
  }

end PlaygroundEngineTest
