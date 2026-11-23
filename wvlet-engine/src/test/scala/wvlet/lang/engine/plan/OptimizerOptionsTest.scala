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
package wvlet.lang.engine.plan

import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

class OptimizerOptionsTest extends UniTest:
  test("should parse comma-separated optimization names") {
    val o = OptimizerOptions.parse(" factor_or, parallelize ,")
    o.disabled shouldBe Set(Optimization.FactorOr, Optimization.Parallelize)
    o.isEnabled(Optimization.FactorOr) shouldBe false
    o.isEnabled(Optimization.ReorderJoins) shouldBe true
  }

  test("should enable every optimization by default") {
    Optimization.values.forall(OptimizerOptions().isEnabled) shouldBe true
    OptimizerOptions.parse("").disabled shouldBe Set.empty
  }

  test("should reject an unknown optimization name") {
    val e = intercept[WvletLangException] {
      OptimizerOptions.parse("factor_or,join_order")
    }
    e.getMessage shouldContain "join_order"
    e.getMessage shouldContain "reorder_joins"
  }

  test("should keep the optimization names stable") {
    Optimization.values.map(_.name).toSeq shouldBe
      Seq(
        "factor_or",
        "decorrelate",
        "decorrelate_correlated",
        "decorrelate_scalar",
        "place_semi_joins",
        "push_down_filters",
        "reorder_joins",
        "exact_join_order",
        "swap_build_side",
        "prefetch_scalar_subqueries",
        "share_ctes",
        "push_down_cte_filters",
        "prune_columns",
        "push_scan_filters",
        "parallelize"
      )
  }

  test("should plan through the runtime plan by default") {
    OptimizerOptions().planner shouldBe PlannerMode.Runtime
    OptimizerOptions().withPlanner(PlannerMode.Legacy).planner shouldBe PlannerMode.Legacy
  }

  test("should parse planner names") {
    OptimizerOptions.parsePlanner(" Runtime ") shouldBe PlannerMode.Runtime
    OptimizerOptions.parsePlanner("legacy") shouldBe PlannerMode.Legacy
    val e = intercept[WvletLangException] {
      OptimizerOptions.parsePlanner("cascades")
    }
    e.getMessage shouldContain "cascades"
    e.getMessage shouldContain "legacy, runtime"
  }

end OptimizerOptionsTest
