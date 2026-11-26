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
        "count_in_subqueries",
        "push_down_filters",
        "join_key_filters",
        "outer_to_inner",
        "reorder_joins",
        "commute_left_joins",
        "reduce_semi_joins",
        "self_join_aggregate",
        "group_key_semi_joins",
        "chain_grouping_sets",
        "prefix_like_selectivity",
        "constant_list_selectivity",
        "exact_join_order",
        "swap_build_side",
        "aggregate_input_rows",
        "prefetch_scalar_subqueries",
        "having_totals_from_groups",
        "share_in_subquery_groups",
        "share_ctes",
        "push_down_cte_filters",
        "cte_filters_through_filters",
        "replan_materialized_ctes",
        "replan_bound_cte_joins",
        "replan_semi_join_builds",
        "replan_cte_bodies",
        "replan_shared_inputs",
        "cte_reader_stats",
        "prune_columns",
        "push_scan_filters",
        "scan_row_filters",
        "selective_scan_row_filters",
        "runtime_join_filters",
        "row_join_filters",
        "build_side_join_filters",
        "scan_join_row_filters",
        "join_row_filters_first",
        "presize_aggregates",
        "window_top_n",
        "window_top_n_stream",
        "running_window_kernels",
        "window_sort_kernels",
        "window_share_partitions",
        "parallelize",
        "late_gather",
        "short_circuit_filters",
        "reorder_conjuncts",
        "join_direct_index",
        "parallel_join_build",
        "concurrent_join_builds",
        "concurrent_shared_ctes",
        "parallel_above_breakers",
        "parallel_agg_partitioning",
        "gpu_offload",
        "parquet_footer_cache",
        "common_subexpressions",
        "shared_agg_partials",
        "adaptive_partial_aggs",
        "pack_agg_keys",
        "stream_pack_agg_keys",
        "long_decimal_keys",
        "dictionary_group_keys",
        "dictionary_entry_scans",
        "dictionary_filters",
        "dictionary_selection",
        "join_build_string_codes",
        "fold_constants",
        "fold_bound_constants",
        "unwrap_casts",
        "set_op_joins",
        "or_implied_filters",
        "union_all_pipelines",
        "semi_join_build_side",
        "decimal_float_scan_filters",
        "range_selectivity",
        "left_group_key_semi_joins",
        "eager_aggregation",
        "eager_grouping_sets",
        "eager_other_side_min_max",
        "primitive_result_distinct",
        "fused_accumulators",
        "masked_case_when",
        "join_tree_build_estimates",
        "const_case_select",
        "case_agg_filters",
        "one_row_partials",
        "substring_in_lists",
        "deferred_null_checks",
        "string_value_selectivity",
        "column_implied_row_filters",
        "aggregate_selected_rows",
        "dense_selected_rows",
        "long_group_tables",
        "sorted_group_runs",
        "clustered_group_estimates"
      )
  }

end OptimizerOptionsTest
