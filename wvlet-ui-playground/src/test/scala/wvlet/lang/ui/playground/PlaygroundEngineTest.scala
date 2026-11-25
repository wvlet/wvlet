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

  test("should run a query its SQL dialect can't express") {
    val out = engine.run(
      Examples.find("04_joins/06_asof_join.wv").get.text,
      dialect = SqlDialect.Trino
    )
    out.error shouldBe None
    out.sql shouldContain "Cannot generate Trino SQL"
    out.result.get.rows.size shouldBe 4
  }

  test("should round doubles for display") {
    PlaygroundEngine.formatDouble(35893.86530718333) shouldBe "35893.865307"
    PlaygroundEngine.formatDouble(2.0) shouldBe "2.0"
    PlaygroundEngine.formatDouble(0.1 + 0.2) shouldBe "0.3"
  }

  test("should show the engine's plan stages and fired rules") {
    val out = engine.run(
      "from lineitem join orders on l_orderkey = o_orderkey where o_totalprice > 1000 count"
    )
    out.error shouldBe None
    out.boundPlan.nonEmpty shouldBe true
    out.runtimePlan.nonEmpty shouldBe true
    out.firedRules.nonEmpty shouldBe true
  }

  test("should run with optimizations turned off") {
    val q   = "from lineitem join orders on l_orderkey = o_orderkey where o_totalprice > 1000 count"
    val on  = engine.run(q)
    val off = engine.run(q, disabled = wvlet.lang.engine.plan.Optimization.values.toSet)
    off.error shouldBe None
    off.result.get.rows shouldBe on.result.get.rows
    off.firedRules shouldBe Nil
  }

  test("should parse plan lines") {
    val l = PlanLine.parse("  ↙ 3) InnerJoin: - (line:3.1)\n    - JoinOn(Eq(a, b))\n    - false")
    l.size shouldBe 2
    l.head.name shouldBe "InnerJoin"
    l.head.sourceLine shouldBe Some(3)
    l(1).isAttribute shouldBe true
    val r = PlanLine.parse("↓ Filter((#1 > 3)) -> [a#1, b#2]").head
    r.name shouldBe "Filter"
    r.detail shouldBe "((#1 > 3))"
    r.outputs shouldBe "[a#1, b#2]"
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
