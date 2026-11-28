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
        "prefetch_scalar_subqueries",
        "share_subquery_scans",
        "share_duplicate_subqueries",
        "folder_file_scans",
        "dataset_file_scans",
        "partition_row_filters",
        "iceberg_row_groups",
        "parquet_footer_cache",
        "planning_memo",
        "match_copied_subquery_scans",
        "decorrelate",
        "place_semi_joins",
        "count_in_subqueries",
        "chain_grouping_sets",
        "having_totals_from_groups",
        "share_in_subquery_groups",
        "reuse_in_subquery_aggregates",
        "fold_bound_constants",
        "unwrap_casts",
        "round_unwrapped_literals",
        "factor_or",
        "push_down_filters",
        "join_key_filters",
        "outer_to_inner",
        "reorder_joins",
        "commute_left_joins",
        "reduce_semi_joins",
        "self_join_aggregate",
        "group_key_semi_joins",
        "aggregate_windows",
        "prune_columns",
        "or_implied_filters",
        "left_group_key_semi_joins",
        "eager_aggregation",
        "eager_other_side_min_max",
        "dependent_group_keys",
        "eager_aggregation_in_join_trees",
        "eager_lookup_projection",
        "dimension_group_ids",
        "late_columns",
        "eager_distinct",
        "implied_join_equalities",
        "prefix_like_selectivity",
        "constant_list_selectivity",
        "exact_join_order",
        "swap_build_side",
        "aggregate_input_rows",
        "semi_join_build_side",
        "range_selectivity",
        "join_tree_build_estimates",
        "string_value_selectivity",
        "clustered_group_estimates",
        "key_join_estimates",
        "probe_scan_cost",
        "join_probe_scan_cost",
        "partition_probe_scan_cost",
        "smaller_outer_builds",
        "or_equals_selectivity",
        "nested_group_estimates",
        "rank_filter_rows",
        "share_ctes",
        "push_down_cte_filters",
        "cte_filters_through_filters",
        "replan_materialized_ctes",
        "replan_bound_cte_joins",
        "replan_semi_join_builds",
        "replan_cte_bodies",
        "replan_shared_inputs",
        "cte_reader_stats",
        "concurrent_shared_ctes",
        "eager_grouping_sets",
        "primitive_result_distinct",
        "cte_determining_join_keys",
        "parallel_result_stats",
        "push_scan_filters",
        "scan_row_filters",
        "selective_scan_row_filters",
        "sparse_key_pass_rates",
        "key_set_row_groups",
        "parallel_column_decode",
        "short_circuit_filters",
        "reorder_conjuncts",
        "dictionary_entry_scans",
        "lazy_dictionary_strings",
        "dictionary_filters",
        "dictionary_selection",
        "decimal_float_scan_filters",
        "substring_in_lists",
        "deferred_null_checks",
        "column_implied_row_filters",
        "parallel_string_decode",
        "runtime_join_filters",
        "row_join_filters",
        "build_side_join_filters",
        "scan_join_row_filters",
        "join_row_filters_first",
        "unmatched_left_probe",
        "probe_miss_filter",
        "left_match_prefilter",
        "narrow_join_entries",
        "join_direct_index",
        "parallel_join_build",
        "concurrent_join_builds",
        "join_build_string_codes",
        "set_op_joins",
        "parallel_outer_tail",
        "semi_anti_one_probe",
        "string_code_probe",
        "parallel_join_keys",
        "slice_single_batch_probes",
        "presize_aggregates",
        "parallel_agg_partitioning",
        "shared_agg_partials",
        "adaptive_partial_aggs",
        "pack_agg_keys",
        "stream_pack_agg_keys",
        "long_decimal_keys",
        "dictionary_group_keys",
        "fused_accumulators",
        "case_agg_filters",
        "dictionary_agg_filters",
        "routed_agg_filters",
        "one_row_partials",
        "aggregate_selected_rows",
        "agg_scratch_buffers",
        "long_group_tables",
        "sorted_group_runs",
        "sorted_agg_splits",
        "union_distinct_once",
        "column_major_fused_sums",
        "packed_agg_routing",
        "window_top_n",
        "window_top_n_stream",
        "running_window_kernels",
        "window_sort_kernels",
        "window_share_partitions",
        "late_gather",
        "common_subexpressions",
        "fold_constants",
        "masked_case_when",
        "const_case_select",
        "dense_selected_rows",
        "fused_decimal_arith",
        "cse_hashed_keys",
        "parallelize",
        "parallel_above_breakers",
        "gpu_offload",
        "union_all_pipelines",
        "size_partitions_by_rows",
        "memory_accounting",
        "fair_task_scheduling",
        "fair_coordinators",
        "spill_aggregation",
        "release_cte_results"
      )
  }

end OptimizerOptionsTest
