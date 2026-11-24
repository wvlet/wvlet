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
        "place_semi_joins",
        "push_down_filters",
        "join_key_filters",
        "reorder_joins",
        "reduce_semi_joins",
        "group_key_semi_joins",
        "exact_join_order",
        "swap_build_side",
        "prefetch_scalar_subqueries",
        "share_ctes",
        "push_down_cte_filters",
        "replan_materialized_ctes",
        "replan_bound_cte_joins",
        "replan_semi_join_builds",
        "replan_cte_bodies",
        "replan_shared_inputs",
        "cte_reader_stats",
        "prune_columns",
        "push_scan_filters",
        "runtime_join_filters",
        "presize_aggregates",
        "parallelize",
        "late_gather",
        "join_direct_index",
        "parallel_join_build",
        "concurrent_join_builds",
        "concurrent_shared_ctes",
        "parallel_above_breakers",
        "parallel_agg_partitioning",
        "gpu_offload"
      )
  }

end OptimizerOptionsTest
