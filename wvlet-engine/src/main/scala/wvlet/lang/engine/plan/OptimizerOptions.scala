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

import wvlet.lang.api.StatusCode

/**
  * An optimization of the engine planner that can be turned off. The names are stable: they are the
  * values of `wvlet.engine.optimizer.disabled`, and the optimizer passes reuse them.
  *
  * The cases are grouped by the planning phase that reads them, in the order a query is planned. A
  * new case goes at the end of its phase's group
  */
enum Optimization(val name: String):
  // --- Plan setup: sources, shared subqueries and scans, and planning memos (before binding) ---

  /** Evaluate uncorrelated scalar subqueries over the same input as one query */
  case PrefetchScalarSubqueries extends Optimization("prefetch_scalar_subqueries")

  /**
    * Read a table once, filtered by the conjuncts an uncorrelated scalar subquery and the rest of
    * the query share, as a CTE of the columns the query names, when the conjuncts keep few rows:
    * the subquery computes it at plan time, and the other reads scan it (TPC-H q22, TPC-DS q44)
    */
  case ShareSubqueryScans extends Optimization("share_subquery_scans")

  /**
    * Compute identical aggregating subqueries of a FROM list once, as a shared CTE that each copy
    * reads under its own alias (TPC-DS q65, [[DuplicateSubqueries]])
    */
  case ShareDuplicateSubqueries extends Optimization("share_duplicate_subqueries")

  /**
    * A folder (or glob) of plain Parquet files, without partition folders or Iceberg / Delta
    * metadata and whose files all have its columns, is read as its files' single-file sources
    * (`catalog/ParquetFolder.scala`): a scan splits into every file's row groups and tests a row
    * filter while decoding, and planning gets row counts, ranges and samples from the files'
    * footers. Otherwise uni's dataset reader reads the folder on one thread without statistics:
    * TPC-DS q17 over a folder of store_sales files took ~10x its single-file time
    */
  case FolderFileScans extends Optimization("folder_file_scans")

  /**
    * A local Iceberg table is read file by file (`catalog/DatasetFiles.scala`): a scan splits into
    * the data files that partition values and manifest statistics don't skip, each read through
    * uni's dataset reader (which applies deletes, field-ID column mapping and defaults) with an
    * exact row filter, and planning gets row counts, ranges and per-file part ranges from the
    * manifests. A folder whose files hold only some of its columns (with the same types, in any
    * order) is read as its files (`ParquetFolder`), missing columns as NULLs. Otherwise uni's
    * dataset reader reads the whole table on one thread without statistics
    */
  case DatasetFileScans extends Optimization("dataset_file_scans")

  /**
    * With `folder_file_scans`, a folder of Hive partitions read as its files takes a row filter, as
    * a single file does: each file decides the conjuncts on partition keys by its values, skipping
    * the file when they reject it, and tests the rest while decoding. Per-row join filters on other
    * keys (store_sales' tickets in TPC-DS q17, q25) and pushed-down conditions then skip decoding
    * the rows they drop, rather than decoding every row and filtering after
    */
  case PartitionRowFilters extends Optimization("partition_row_filters")

  /**
    * An Iceberg table whose data files all read as plain Parquet files (uni's
    * `ParquetDataset.readsAsPlainFile`: no deletes, no renamed or dropped columns, no defaults) is
    * read as a folder of single files (`ParquetFolder`) instead of file by file through uni's
    * dataset reader: a scan splits into every file's row groups and tests a row filter while
    * decoding, and planning reads the files' footers. Needs `dataset_file_scans`
    */
  case IcebergRowGroups extends Optimization("iceberg_row_groups")

  /**
    * Keep the parsed footers of local Parquet files across opens, keyed by path and checked against
    * the file's size and last-modified time ([[wvlet.lang.engine.catalog.ParquetFooterCache]]), so
    * that planning and each scan don't parse the same footer again (DuckDB's
    * `parquet_metadata_cache`)
    */
  case ParquetFooterCache extends Optimization("parquet_footer_cache")

  /**
    * Planning remembers what it computes more than once per query (`planning_memo`): whether a
    * subquery is correlated (it plans the subquery to see whether a column is missing), and the SQL
    * text [[wvlet.lang.engine.plan.Planner]] matches OR branches' conjuncts by. The binder's
    * missing-column signal, which these checks and `bindsIn` catch, carries no stack trace. Small
    * TPC-DS queries with a correlated scalar subquery (q32, q41, q92) planned it two or three
    * times, and filling in the stack traces of ~75 missed bindings took ~5% of their planning
    */
  case PlanningMemo extends Optimization("planning_memo")

  /**
    * A filter whose scan is shared (`share_subquery_scans`) is found by value with its templates
    * expanded too, not only as the tree's node. Splitting a condition into conjuncts expands its
    * templates, which copies the subqueries in it: TPC-H q22's `avg(c_acctbal)` subquery was bound
    * as such a copy and read `customer` again instead of the shared scan
    */
  case MatchCopiedSubqueryScans extends Optimization("match_copied_subquery_scans")

  // --- Binding: subqueries, aggregations, and constants bound into the runtime plan ------------

  /**
    * Bind [NOT] EXISTS / [NOT] IN filter conjuncts over uncorrelated subqueries as semi and anti
    * joins on the runtime plan; disabled, they are plan-time constants in a filter (a value set for
    * [NOT] IN), evaluated with SQL NULL semantics
    */
  case Decorrelate extends Optimization("decorrelate")

  /**
    * Join an uncorrelated IN conjunct over an inner join tree that reads one input, over a subquery
    * no larger than it, onto that input before join order, instead of over the ordered tree (with
    * `decorrelate`)
    */
  case PlaceSemiJoins extends Optimization("place_semi_joins")

  /**
    * With `place_semi_joins`, compute an uncorrelated IN subquery estimated larger than the input
    * it would join at plan time, and place its semi join by the exact row count, building on the
    * computed rows (estimates pass through aggregations, e.g. TPC-H q18's group by having)
    */
  case CountInSubqueries extends Optimization("count_in_subqueries")

  /**
    * Plan ROLLUP / CUBE (grouping sets whose key union is one of the sets) by aggregating the input
    * once, by the finest set, into partial results, then each coarser set from the partial results
    * of its smallest finer set, instead of one aggregation over the whole input per set
    */
  case ChainGroupingSets extends Optimization("chain_grouping_sets")

  /**
    * Compute an aggregation's groups at plan time when scalar subqueries of its condition aggregate
    * the same input, and total the subqueries' sums, mins, and maxes over the groups instead of
    * reading the input again (TPC-H q11)
    */
  case HavingTotalsFromGroups extends Optimization("having_totals_from_groups")

  /**
    * Compute an aggregation's groups at plan time when its input is filtered by `key in (subquery)`
    * on a grouping key and the subquery aggregates the same input (by keys insensitive to
    * inner-join order and table aliases): the subquery merges the groups, which the filter then
    * applies to, instead of reading the input twice (TPC-DS q70)
    */
  case ShareInSubqueryGroups extends Optimization("share_in_subquery_groups")

  /**
    * Replace a table of an aggregation's inner join by the groups of an IN subquery that groups it
    * by the join key, when the aggregation only sums, mins, or maxes its columns: the aggregation
    * totals the subquery's per-key values instead of reading the table again (TPC-H q18)
    */
  case ReuseInSubqueryAggregates extends Optimization("reuse_in_subquery_aggregates")

  /**
    * Expressions over literals become literals as they are bound (`1200 + 11`, `cast(1 + 10 as
    * decimal(21,2))`), so that filter pushdown and scan filters see the folded bounds
    * ([[BoundExprSimplifier.fold]])
    */
  case FoldBoundConstants extends Optimization("fold_bound_constants")

  /**
    * A comparison of a lossless cast with literals (`cast(x as decimal(12,2)) > 50.00`) compares
    * the uncast expression with the literals converted to its type, when they convert exactly
    * ([[BoundExprSimplifier.unwrapCompare]])
    */
  case UnwrapCasts extends Optimization("unwrap_casts")

  /**
    * With `unwrap_casts`, a comparison of a cast that only adds fraction digits to an integer or
    * decimal `x` with a literal (`cast(x as decimal(38,12)) > 81029137.652468`) compares x with the
    * literal rounded to x's scale: down for `>` and `<=`, up for `>=` and `<`, since x's values lie
    * on that grid. Such casts keep every value (the engine never enforces a decimal precision), so
    * they unwrap even when the target has fewer integer digits. TPC-H q11's HAVING filter cast each
    * of its plan-time groups through BigDecimal on the calling thread
    * ([[BoundExprSimplifier.unwrapCompare]])
    */
  case RoundUnwrappedLiterals extends Optimization("round_unwrapped_literals")

  // --- Logical passes over the runtime plan (`Optimizer`) --------------------------------------

  /** `(a and b) or (a and c)` => `a and (b or c)` */
  case FactorOr extends Optimization("factor_or")

  /**
    * Push filter conjuncts down the runtime plan: through projections, aggregates (grouping keys
    * only), and sorts, into the preserved side of outer joins and into inner join trees, with
    * equalities to constants inferred transitively there
    */
  case PushDownFilters extends Optimization("push_down_filters")

  /**
    * With `push_down_filters`, carry a conjunct over the join keys of one input of a hash join to
    * the other input, with the keys replaced by their partners: from the preserved input of a LEFT
    * or RIGHT join, from the left input of a semi or anti join, and both ways for an inner join.
    * Conjuncts also move into the inputs of inner joins the binder already ordered
    */
  case JoinKeyFilters extends Optimization("join_key_filters")

  /**
    * With `push_down_filters`, turn a LEFT join into an inner join when a filter over it, or a
    * condition of the inner join tree it is an input of, is never TRUE on a NULL-padded row, so
    * that [[ReorderJoins]] orders its inputs with the other inner joins (TPC-DS q93)
    */
  case OuterToInner extends Optimization("outer_to_inner")

  /** Cost-based order of inner joins; disabled, the inputs are joined in written order */
  case ReorderJoins extends Optimization("reorder_joins")

  /**
    * Move a LEFT join that is an input of an inner join tree above the tree when no condition of
    * the tree reads its right input, so that the inner joins reduce its left input before it probes
    * or builds (TPC-DS q80, q40). Its build side is then chosen over the joined tree's estimated
    * rows. Only with `reorder_joins`, which otherwise keeps the written order
    */
  case CommuteLeftJoins extends Optimization("commute_left_joins")

  /**
    * Where only the set of rows matters (the subquery side of a semi or anti join, the input of a
    * DISTINCT or of a group-by without aggregates, and a shared CTE all of whose readers are such),
    * turn an inner join whose needed columns come from one side into a semi join of that side
    */
  case ReduceSemiJoins extends Optimization("reduce_semi_joins")

  /**
    * Replace a semi join of a table with itself on an equal key and a different value (`t1.k = t2.k
    * and t1.x <> t2.x`, both sides whole scans) by a semi join on the key with the keys whose group
    * holds two different values (`group by k having min(x) <> max(x)`), or, where only the distinct
    * keys matter, by those keys alone (TPC-DS q95). Applied by [[ReduceSemiJoins]]' walk, so it
    * needs `reduce_semi_joins` too
    */
  case SelfJoinAggregate extends Optimization("self_join_aggregate")

  /**
    * Filter the input of an aggregation that a join reads by a group key to the keys a small
    * relation on the join's other side holds, with a semi join, so that the groups the join can
    * never match are not computed
    */
  case GroupKeySemiJoins extends Optimization("group_key_semi_joins")

  /**
    * Compute a min, max, or avg per key that a join reads back over the join's own rows, as a
    * window partitioned by the key, when those rows are the aggregation's input joined with other
    * relations on the key alone (TPC-H q17, q2): the input is read once ([[AggregateWindows]])
    */
  case AggregateWindows extends Optimization("aggregate_windows")

  /** Read and pass only the columns the operators above use */
  case PruneColumns extends Optimization("prune_columns")

  /**
    * With `push_down_filters`, an `or` condition of an inner join tree that reads several inputs
    * also filters each input by the `or`, over its branches, of the conjuncts that read only that
    * input, when every branch has one (TPC-H q19's part filter, q7's nation names). The `or` stays,
    * so the rows are the same ([[PushDownFilters]])
    */
  case OrImpliedFilters extends Optimization("or_implied_filters")

  /**
    * With `group_key_semi_joins`, an aggregation on the left (probe) input of an inner join is
    * reduced too, as the join's inputs swap with the same build side (TPC-H q20 once its lineitem
    * aggregation is estimated larger than the partsupp side)
    */
  case LeftGroupKeySemiJoins extends Optimization("left_group_key_semi_joins")

  /**
    * Aggregate the side of an inner join that an aggregation's calls read by its join keys and the
    * group keys it holds before the join, when that is estimated to reduce it at least 4 times and
    * the other side keeps most of its rows: the join then probes, and gathers the other side's
    * group keys for, one row per partial group (TPC-DS q04, q11, q74 sum the sales per customer key
    * and year before joining the seven customer strings they group by)
    */
  case EagerAggregation extends Optimization("eager_aggregation")

  /**
    * With [[EagerAggregation]], also aggregate one side of the join eagerly when the other calls
    * are mins and maxes of the other side's columns, which the outer aggregation computes over the
    * joined rows as before (duplicates don't change them), and when the other side is a cross join
    * with one row: TPC-DS q23's best customers sum 27M store sales per customer key before joining
    * the customers crossed with the scalar maximum they compare the sums to
    */
  case EagerOtherSideMinMax extends Optimization("eager_other_side_min_max")

  /**
    * An aggregation groups only by the keys that determine its others through declared unique keys
    * and inner join equalities, carrying the rest as `any_value` of each group
    * ([[wvlet.lang.engine.runtime.DependentGroupKeys]]): TPC-H q10 groups by `c_custkey` alone
    * instead of seven customer columns and the nation name
    */
  case DependentGroupKeys extends Optimization("dependent_group_keys")

  /**
    * With [[EagerAggregation]], aggregate an input deeper in the join tree under an aggregation
    * before the joins above it, when the calls read that input only, every join above looks up one
    * row by a declared unique key, and the group keys determine the input's: the outer aggregation
    * becomes a projection ([[wvlet.lang.engine.runtime.EagerAggregation]]). TPC-H q10 sums its
    * order lines per `o_custkey` before looking up customer and nation, so the customer join builds
    * 380K rows instead of 1.15M at SF10
    */
  case EagerAggregationInJoinTrees extends Optimization("eager_aggregation_in_join_trees")

  /**
    * With [[EagerAggregation]], when the join under an aggregation looks up at most one row of its
    * other side by a declared unique key, and the group keys determine the partial groups, the
    * outer aggregation becomes a projection of the partial results: each outer group is one partial
    * group joined to one row. TPC-DS q04, q11 and q74 sum their sales per customer key and year,
    * look up the customer, and no longer group the sums again by `c_customer_id` and year
    */
  case EagerLookupProjection extends Optimization("eager_lookup_projection")

  /**
    * An aggregation grouped by string columns of a dimension that a larger input looks up by its
    * declared unique key groups by a representative key of those strings instead (`min(key) over
    * (partition by strings)` over the dimension's rows), and joins the strings back per group
    * ([[wvlet.lang.engine.runtime.DimensionGroupIds]]): TPC-DS q47 and q89 group store sales by two
    * item and two store strings
    */
  case DimensionGroupIds extends Optimization("dimension_group_ids")

  /**
    * A sort with a limit reads the string columns of a scan that only pass through to its output
    * for its N rows alone: the top-N is computed without them, and a second scan of the table,
    * filtered by the N rows' declared unique key, joins them back
    * ([[wvlet.lang.engine.runtime.LateColumns]]). TPC-H q10 decoded the name, address, phone and
    * comment of 381K customers for its top 20
    */
  case LateColumns extends Optimization("late_columns")

  /**
    * With [[EagerAggregation]], a DISTINCT (an aggregation without calls) over a projection of an
    * inner join is computed over the join with the projection inlined into its keys, so that the
    * join's input is first deduplicated by the columns the keys and join keys read. TPC-DS q38 and
    * q87 deduplicate 5.4M store sales rows by customer key and date (481K at SF10) before joining
    * the customer names, instead of gathering and hashing the names of every sale row
    */
  case EagerDistinct extends Optimization("eager_distinct")

  /**
    * The join search also joins inputs by the equalities that follow from the written ones: column
    * equalities of inner joins form equivalence classes, and every two columns of a class on
    * different inputs may be a join key ([[wvlet.lang.engine.plan.JoinEdge.eqClass]]). A join keys
    * on one column pair per class, and its rows are estimated once per class. TPC-H q5 writes
    * `c_nationkey = s_nationkey and s_nationkey = n_nationkey`, so customer joined only through
    * supplier and came last, after the engine built all 2.3M orders of the year; with
    * `c_nationkey = n_nationkey`, ASIA's 300K customers filter the orders first, as in DuckDB
    */
  case ImpliedJoinEqualities extends Optimization("implied_join_equalities")

  // --- Estimates and join order (`Stats`, the join search) -------------------------------------

  /**
    * Estimate a prefix pattern `x like 'p%'` as the range `x >= 'p' and x < next(p)` it is, two
    * range conditions of the default selectivity each, instead of one
    */
  case PrefixLikeSelectivity extends Optimization("prefix_like_selectivity")

  /**
    * Estimate `x = c` and `x in (c1, ...)` by the distinct values of x when the compared values are
    * constant expressions (`1999 + 1`), not only literals, so a dimension filtered by such a list
    * reads as small as it is and an aggregated star join over it builds the final join (TPC-DS q68,
    * q46's `d_year in (1999, 1999 + 1, 1999 + 2)`)
    */
  case ConstantListSelectivity extends Optimization("constant_list_selectivity")

  /**
    * Enumerate the join trees of small inner join graphs exactly, including bushy trees, instead of
    * multi-start greedy search
    */
  case ExactJoinOrder extends Optimization("exact_join_order")

  /** Build the hash table on the smaller input of a join */
  case SwapBuildSide extends Optimization("swap_build_side")

  /**
    * Estimate an aggregated input of an inner join graph from the joins and filters below its
    * aggregation, instead of the rows of its probe source, so that a join with an aggregated star
    * join builds on the aggregate (TPC-DS q73, q34, q68, q46)
    */
  case AggregateInputRows extends Optimization("aggregate_input_rows")

  /**
    * With `swap_build_side`, a semi / anti join of an EXISTS / IN subquery builds on the outer
    * input (a RIGHT SEMI / RIGHT ANTI join) whenever the subquery input is estimated larger, not
    * only [[JoinOrderOptimizer.SwapBuildRatio]] times larger: a swapped semi join outputs only the
    * outer columns, so it needs no reordering projection, and its build keys filter the larger
    * probe scan (TPC-H q4, q21 build on orders instead of lineitem)
    */
  case SemiJoinBuildSide extends Optimization("semi_join_build_side")

  /**
    * Estimate a range comparison of an integer or date column with a constant (`<`, `>`, `between`,
    * date ranges) from the column's [min, max] in the Parquet statistics, with the bounds on one
    * column combined into one interval ([[wvlet.lang.engine.runtime.RangeSelectivity]]), instead of
    * the default selectivity per comparison
    */
  case RangeSelectivity extends Optimization("range_selectivity")

  /**
    * Choose the build side of the joins planned outside the join search (EXISTS / IN semi and anti
    * joins, outer joins, and the semi joins of `reduce_semi_joins` and `group_key_semi_joins`) from
    * join-tree estimates ([[wvlet.lang.engine.runtime.Stats.treeRows]]): filters by their
    * selectivity and inner joins as the join search estimates them, instead of the probe source's
    * rows. The semi joins of `reduce_semi_joins` and `group_key_semi_joins` then build on the
    * smaller side by any margin, as `semi_join_build_side` does for subqueries (TPC-DS q16 builds
    * its EXISTS join on the filtered outer orders instead of catalog_sales)
    */
  case JoinTreeBuildEstimates extends Optimization("join_tree_build_estimates")

  /**
    * Estimate string `=`, IN, LIKE and IS NULL conditions (and AND / OR / NOT of them) over one
    * column of a Parquet file from the values of a few sampled row groups
    * ([[wvlet.lang.engine.catalog.ValueCounts]]), cached per file version, for the scan row-filter
    * gate (`selective_scan_row_filters`) and the join search, instead of 1 / ndv or the default 0.3
    * (TPC-H q3's `c_mktsegment = 'BUILDING'` keeps 20%)
    */
  case StringValueSelectivity extends Optimization("string_value_selectivity")

  /**
    * Group counts of rows that come in clusters sharing their keys, such as the lines of an order
    * sharing its customer and date, are estimated over the clusters instead of the rows
    * ([[wvlet.lang.engine.runtime.ClusteredGroups]]), with cluster sizes from the mean run length
    * of the key columns in sampled Parquet row groups. They feed the join-tree aggregation
    * estimates and the eager-aggregation guard: TPC-DS q74's web (customer, year) groups were
    * estimated at 1M where there are 213K, so its partial aggregation below the customer join was
    * skipped
    */
  case ClusteredGroupEstimates extends Optimization("clustered_group_estimates")

  /**
    * The join search estimates an equi-join on several columns from the unique keys its tables
    * declare ([[wvlet.lang.engine.catalog.TableSource.uniqueKeys]]): keys covering a declared key
    * of one side join as a foreign key to that primary key, and keys that are no declared key of
    * either side as independent columns, without the cap that treats them as one correlated key.
    * TPC-DS q25's catalog_sales ⋈ store_returns on (item, customer) was estimated at 35K rows for
    * 13 real ones
    */
  case KeyJoinEstimates extends Optimization("key_join_estimates")

  /**
    * The exact join search charges each join whose probe side is one input for the probe rows the
    * runtime join filters of its build are expected to keep (the smallest ratio of build to probe
    * distinct keys), so that it prefers orders that shrink a large fact probe early: TPC-DS q25
    * joins catalog_sales with its date filter before store_returns, so the scan skips row groups
    */
  case ProbeScanCost extends Optimization("probe_scan_cost")

  /**
    * With [[ProbeScanCost]], a probe side that is itself a join is charged too, for its output rows
    * that the build's runtime join filters are expected to keep (its distinct keys capped by the
    * rows of the joins holding them). Otherwise only single-input probes pay, so the search builds
    * on a large single input to make it no probe: TPC-H q10 built on customer, all seven of its
    * grouped columns, rather than probing it with the filtered order lines
    */
  case JoinProbeScanCost extends Optimization("join_probe_scan_cost")

  /**
    * The exact join search credits a probe scan with the parts it skips by a build's join keys,
    * when every part of the scan (a Hive partition, or a row group) holds one value of its key
    * column, as the partitions of a date-partitioned table do: the scan reads only the parts
    * holding build keys (`key_set_row_groups`), the build keys' share of the column's values. A
    * runtime join filter on another key reads every part and drops rows after decoding them, so
    * without the credit TPC-DS q25 over store_sales in date partitions built store_sales ⋈
    * (catalog_sales ⋈ store_returns) and probed it with d1's 30 dates, reading all 1823 partitions
    * instead of 30. Needs `key_set_row_groups`
    */
  case PartitionProbeScanCost extends Optimization("partition_probe_scan_cost")

  /**
    * An outer join builds on its other input once its build input is estimated
    * [[JoinOrderOptimizer.SwapOuterBuildRatio]] times larger instead of
    * [[JoinOrderOptimizer.SwapBuildRatio]], as `parallel_outer_tail` (which it needs) emits a RIGHT
    * join's unmatched build rows in parallel. The build's keys then filter the larger input's scan
    * (TPC-DS q75 builds on the filtered sales rows and probes the returns)
    */
  case SmallerOuterBuilds extends Optimization("smaller_outer_builds")

  /**
    * An `or` of comparisons of one expression with literals (`d_year = 2002 or d_year = 2001`)
    * keeps n / ndv of the rows, as `x in (n literals)` does, instead of the default selectivity.
    * TPC-DS q75's CTE readers push such an `or` into its body
    */
  case OrEqualsSelectivity extends Optimization("or_equals_selectivity")

  /**
    * The estimated groups of an aggregation read its keys' distinct values through the grouping
    * keys of an aggregation below it (`Stats.treeRows`), instead of taking the inner aggregation's
    * rows: TPC-DS q65's average revenue by store, over sums by store and item, is about 100 rows,
    * not the 5.8M of the sums, so the join builds on it
    */
  case NestedGroupEstimates extends Optimization("nested_group_estimates")

  /**
    * An input of an inner join graph that filters a row_number() or rank() by a constant (`rnk <
    * 11`) is estimated as at most that many rows per partition of the window, instead of the rows
    * below the window, so that TPC-DS q44's two top-10 rankings are built on and the item scans
    * probe them
    */
  case RankFilterRows extends Optimization("rank_filter_rows")

  /**
    * A string join key with no distinct count (files over `ParquetSource.DistinctScanLimit` rows)
    * takes the distinct values of the column's value sample when the sample has seen almost every
    * value (`ValueCounts.sampledDistinct`), instead of the unknown-keys rule that treats the
    * smaller side as unique: TPC-DS q85 at SF100 joined customer_demographics to itself on
    * (cd_marital_status, cd_education_status) first, estimated 1.9M rows for ~9B
    */
  case SampledStringDistinct extends Optimization("sampled_string_distinct")

  // --- Shared CTEs and results, and replanning -------------------------------------------------

  /** Compute a CTE referenced more than once only once */
  case ShareCTEs extends Optimization("share_ctes")

  /**
    * Filter a shared CTE's body by what its readers' filters agree on, when every reader reports
    * one (with `push_down_filters`)
    */
  case PushDownCTEFilters extends Optimization("push_down_cte_filters")

  /**
    * Move the readers' filters pushed into a shared CTE body below the body's own filters when they
    * can't throw, so they reach the joins and scans under them (TPC-DS q64: `syear in (1999, 2000)`
    * below the `cd_marital_status <>` filter, down to `date_dim`)
    */
  case CTEFiltersThroughFilters extends Optimization("cte_filters_through_filters")

  /**
    * Compute the shared CTEs that joins of the query read once they are planned, and order those
    * joins again with the exact row counts
    */
  case ReplanMaterializedCTEs extends Optimization("replan_materialized_ctes")

  /**
    * With `replan_materialized_ctes`, also order again the joins the binder ordered while binding
    * (aggregation, pivot, and subquery inputs) when they read a computed shared CTE
    */
  case ReplanBoundCTEJoins extends Optimization("replan_bound_cte_joins")

  /**
    * With `replan_materialized_ctes`, also compute the shared CTEs read below semi and anti joins,
    * and choose those joins' build sides again with the exact row counts
    */
  case ReplanSemiJoinBuilds extends Optimization("replan_semi_join_builds")

  /**
    * With `replan_materialized_ctes`, also compute the shared CTEs that joins in another shared
    * CTE's body read, and order that body's joins again with the exact row counts
    */
  case ReplanCTEBodies extends Optimization("replan_cte_bodies")

  /**
    * With `replan_materialized_ctes`, also compute the shared CTEs that joins in a shared input of
    * grouping sets or unpivot read, and order that input's joins again with the exact row counts
    */
  case ReplanSharedInputs extends Optimization("replan_shared_inputs")

  /**
    * With `replan_materialized_ctes`, estimate the reads of a computed shared CTE from its result:
    * the distinct values of its columns, and the rows passing a reader's own filter conditions
    */
  case CTEReaderStats extends Optimization("cte_reader_stats")

  /**
    * With `parallelize`, compute the shared CTE results a plan reads at the same time, each drained
    * on a coordinator thread outside the task pool, instead of one by one on the calling thread
    */
  case ConcurrentSharedCTEs extends Optimization("concurrent_shared_ctes")

  /**
    * Run [[EagerAggregation]] over the body of a shared input the planner builds while binding (the
    * finest aggregation of chained grouping sets ([[ChainGroupingSets]]), and the inputs of other
    * grouping sets and unpivots), which the runtime passes don't visit: TPC-DS q22's ROLLUP of 26M
    * inventory rows by four item strings sums them per item key before joining the 102K items
    */
  case EagerGroupingSets extends Optimization("eager_grouping_sets")

  /**
    * Count the distinct values of an integer column of a shared CTE result computed for planning
    * ([[CTEReaderStats]]) in a set of longs sized for its rows instead of a growing group table:
    * the same counts, so the same plans, for less planning time (TPC-DS q01 counts 540K customer
    * keys when it orders its joins again at SF10)
    */
  case PrimitiveResultDistinct extends Optimization("primitive_result_distinct")

  /**
    * Readers of a shared CTE joined on a string column that copies a declared unique key join on a
    * long declared key of the same table instead, which the body carries as one more column
    * ([[wvlet.lang.engine.runtime.CTEJoinKeys]]). TPC-DS q04, q11 and q74 join their `year_total`
    * readers on `c_customer_sk` instead of `c_customer_id`
    */
  case CTEDeterminingJoinKeys extends Optimization("cte_determining_join_keys")

  /**
    * The statistics of a shared CTE result computed for planning ([[CTEReaderStats]]) are computed
    * by the engine's tasks instead of on the planning thread, and a string column's distinct values
    * are counted by their hash codes and lengths in a set of longs instead of a string group table
    * ([[wvlet.lang.engine.runtime.ParallelResultStats]]). TPC-DS q11 counts its 1M-row `year_total`
    * result's customer ids and its readers' filtered rows at SF10
    */
  case ParallelResultStats extends Optimization("parallel_result_stats")

  // --- Physical: scans and filters -------------------------------------------------------------

  /** Skip Parquet row groups by the filters directly over a scan */
  case PushScanFilters extends Optimization("push_scan_filters")

  /**
    * The conjuncts of a filter directly over a single-file Parquet scan that uni's row filter
    * evaluates exactly move into the scan ([[wvlet.lang.engine.catalog.ScanSpec.rowFilter]]), most
    * selective first: uni decodes their columns first and the other columns only for the matching
    * rows (DuckDB's filter-first Parquet decode). The others stay in the engine's filter
    */
  case ScanRowFilters extends Optimization("scan_row_filters")

  /**
    * With `scan_row_filters`, a scan's exact conjuncts move into its row filter only when they are
    * estimated to keep at most [[ScanFilters.MaxRowFilterSelectivity]] of its rows. uni evaluates
    * the row filter row by row and gathers the other columns for the rows it keeps, which costs
    * more than decoding them all when few rows drop (TPC-H q1's `l_shipdate <= ...` keeps 96%,
    * TPC-DS q09's `ss_quantity between` ranges 20% each)
    */
  case SelectiveScanRowFilters extends Optimization("selective_scan_row_filters")

  /**
    * With `scan_join_row_filters`, the per-row join filters in a probe scan's row filter are
    * ordered by expected pass rates that count the probe column's distinct values rather than the
    * width of its [min, max] range, which overstates a sparse column's values: TPC-H's order keys
    * use a quarter of their range, so q7's `l_orderkey` Bloom filter looked four times as selective
    * as it is and ran ahead of the cheaper and equally selective `l_suppkey` bitmap, testing every
    * lineitem row. Which filters go into the row filter is decided as before
    */
  case SparseKeyPassRates extends Optimization("sparse_key_pass_rates")

  /**
    * Per-row runtime join filters over a bitmap or a few build keys (a Bloom filter over at most
    * `RowKeyFilter.MaxSortedKeys` rows) also skip a Parquet probe scan's row groups whose [min,
    * max] holds no build key, not only those outside the keys' range. TPC-DS q17 and q25 probe
    * store_sales, clustered by ticket number, with the ~15 tickets of a catalog_sales ⋈
    * store_returns build: the range spans most of the file, but the keys hit a few row groups.
    * Needs `row_join_filters`
    */
  case KeySetRowGroups extends Optimization("key_set_row_groups")

  /**
    * A pipeline scan of a Parquet file with a single row group to read (no parts to run in
    * parallel) decodes its columns as tasks of the engine's runner, the calling thread taking one.
    * TPC-H supplier holds 100K rows in one row group at SF10: q2's supplier join build decoded its
    * four string columns on one thread for ~20 ms while every other thread waited
    */
  case ParallelColumnDecode extends Optimization("parallel_column_decode")

  /**
    * A filter over AND / OR evaluates each conjunct only on the rows the earlier ones kept (each
    * disjunct only on the rows not yet accepted), and stops when no row remains (DuckDB's
    * `ExpressionExecutor::Select`)
    */
  case ShortCircuitFilters extends Optimization("short_circuit_filters")

  /**
    * With `short_circuit_filters`, the conjuncts and disjuncts of a filter run cheapest first by a
    * static cost (primitive comparisons before string, IN, LIKE, and function calls); parts that
    * can throw stay in place
    */
  case ReorderConjuncts extends Optimization("reorder_conjuncts")

  /**
    * Parquet scans read the rows of a string column decoded from a dictionary page as their
    * dictionary entries only, without copying each row's bytes (uni's
    * `ParquetReadOptions.dictionaryCodesOnly`): every row's String is the entry's one String
    * anyway, so the per-row copy only fed a buffer nothing reads (TPC-H q1's `l_returnflag` /
    * `l_linestatus`, DuckDB's dictionary vectors). Columns a scan row filter reads are read as
    * before
    */
  case DictionaryEntryScans extends Optimization("dictionary_entry_scans")

  /**
    * Parquet string vectors decoded from a dictionary hold only their rows' codes and their batch's
    * entry strings, building the per-row String array on first use ([[StrVec.ofEntries]]). Grouping
    * by the codes (`dictionary_group_keys`) and per-entry filters never build it, so a scan skips
    * one String store per row (TPC-H q1's `l_returnflag` / `l_linestatus`, DuckDB's dictionary
    * vectors). Needs `dictionary_group_keys`, whose scans keep the codes
    */
  case LazyDictionaryStrings extends Optimization("lazy_dictionary_strings")

  /**
    * Parquet string columns without a dictionary (PLAIN pages) are held as their UTF-8 bytes and
    * offsets ([[StrVec.ofBytes]]), sharing uni's arrays, instead of one String per row. LIKE reads
    * the bytes, so a column used only by a filter never builds Strings (TPC-H q13's `o_comment`);
    * other readers build them once per source vector, and gathers share the bytes through a row
    * index
    */
  case PlainStringBytes extends Optimization("plain_string_bytes")

  /**
    * Filter conditions over one string column, made of comparisons, IN lists, LIKE, and IS NULL,
    * run once per distinct dictionary entry of a batch whose column carries the codes that
    * `dictionary_group_keys` scans keep, and each row takes its entry's result (TPC-H q16's
    * `p_type not like`, DuckDB's dictionary vectors). Without codes, or with more entries than half
    * the rows, they run per row ([[wvlet.lang.engine.expr.PerDictEntry]])
    */
  case DictionaryFilters extends Optimization("dictionary_filters")

  /**
    * In filters that short-circuit AND / OR (`short_circuit_filters`), a `dictionary_filters` part
    * runs once per dictionary entry of the rows still selected and returns their positions, so an
    * AND of such parts looks at fewer rows part by part instead of building a mask over the whole
    * batch for each (TPC-DS q41's OR of ANDs over item's strings, DuckDB's selection vectors)
    */
  case DictionarySelection extends Optimization("dictionary_selection")

  /**
    * With `push_scan_filters`, comparisons of decimal columns with constants, and `<`, `<=` and `=`
    * of floating-point columns, also skip Parquet row groups by their min/max statistics
    * ([[ScanFilters]]). Floating-point `>` and `>=` stay out: writers leave NaN out of the
    * statistics while the engine orders NaN above every number
    */
  case DecimalFloatScanFilters extends Optimization("decimal_float_scan_filters")

  /**
    * `substring(s, k, n) IN ('a', ...)` with constant `k >= 1` and `n` looks up each string's
    * region in a hash table of the values, without building the substrings (TPC-H q22's
    * `c_phone.substring(1, 2).in(...)`)
    */
  case SubstringInLists extends Optimization("substring_in_lists")

  /**
    * IS NULL of a column that a join or filter output has not gathered yet reads the column's row
    * positions (-1 for a padded NULL row) and its source's nulls, without gathering the column
    * (TPC-DS q97's null checks over the FULL join output)
    */
  case DeferredNullChecks extends Optimization("deferred_null_checks")

  /**
    * With `scan_row_filters`, an `or` conjunct of a filter over a scan that reads several columns
    * also gives the scan's row filter, for each column that every branch tests, the `or` over the
    * branches of their conjuncts on that column ([[ScanFilters.columnImplied]]), as DuckDB derives
    * them: uni decodes the other columns only for the rows those keep. The `or` stays in the
    * engine's filter, so the rows are the same (TPC-H q19's part filter keeps 0.24% of the rows but
    * reads three columns, so it was estimated to keep 30% and decoded every row). Estimated by
    * `string_value_selectivity` for the row-filter gate, as string estimates of 0.3 let selective
    * guesses through (#2429)
    */
  case ColumnImpliedRowFilters extends Optimization("column_implied_row_filters")

  /**
    * A Parquet row group decoded in parallel (`parallel_column_decode`) builds the Strings of its
    * PLAIN string columns by row ranges, as tasks of their own after the columns are decoded. TPC-H
    * q2's supplier scan built the 100K Strings of `s_comment` on one thread while the others waited
    */
  case ParallelStringDecode extends Optimization("parallel_string_decode")

  // --- Physical: joins -------------------------------------------------------------------------

  /**
    * Skip Parquet row groups on the probe side of a hash join by the [min, max] range of its build
    * keys, once the build is done
    */
  case RuntimeJoinFilters extends Optimization("runtime_join_filters")

  /**
    * Also drop probe rows whose key is not a build key, per row in the probe scan: a bitmap over
    * dense integer keys or a Bloom filter ([[wvlet.lang.engine.exec.RowKeyFilter]]), paused when it
    * keeps most rows. Needs `runtime_join_filters`
    */
  case RowJoinFilters extends Optimization("row_join_filters")

  /**
    * Runtime join filters also reach the scans of the preserved build input of a LEFT, RIGHT, semi,
    * or anti join below the probe side, not only its probe input: dropping a build row there only
    * drops output rows whose key has no match above. Such a filter is set before the scan reads
    * when the join above builds first (an outer join in a UNION ALL input, TPC-DS q05's
    * `web_returns`); a scan that starts earlier reads without it. Needs `runtime_join_filters`
    */
  case BuildSideJoinFilters extends Optimization("build_side_join_filters")

  /**
    * Per-row runtime join filters expected to keep few rows of a single-file Parquet probe scan go
    * into its row filter as `In` conjuncts, so that uni decodes the other columns only for the rows
    * they accept, instead of decoding every row and dropping them after. Needs `row_join_filters`
    */
  case ScanJoinRowFilters extends Optimization("scan_join_row_filters")

  /**
    * With `scan_join_row_filters`, the per-row join filters go ahead of a scan's own row filter
    * conjuncts when they are expected to keep fewer rows: uni tests the conjuncts in order, each
    * decoding its column for the rows the ones before it kept. TPC-H q19's `l_partkey` filter keeps
    * far fewer lineitem rows than its `l_shipmode` and `l_shipinstruct` conjuncts
    */
  case JoinRowFiltersFirst extends Optimization("join_row_filters_first")

  /**
    * A LEFT hash join under a filter that tests one of its build key columns for NULL emits only
    * its unmatched probe rows, in probe order, with NULL build columns: a matched row's build key
    * equals a non-NULL probe key, so the filter drops every matched pair. The join skips the match
    * pairs, the matched marks and the reordering of unmatched rows after the matched ones, and the
    * columns gathered above read the probe rows in order. TPC-DS q78 keeps the sales rows without a
    * return this way (`where sr_ticket_number is null`)
    */
  case UnmatchedLeftProbe extends Optimization("unmatched_left_probe")

  /**
    * Anti joins and `unmatched_left_probe` LEFT joins, whose probes mostly find no match, give a
    * large flat hash table (at least `LongJoinTable.MinMissFilterBytes` of entries) a Bloom filter
    * of its keys, about 8 bits per key, that each probe checks before the entries: a rejected key
    * reads a cached word instead of an entry out of cache. TPC-DS q78 probes the store_sales rows
    * of one year against the entries of every store_returns row, and most of them miss
    */
  case ProbeMissFilter extends Optimization("probe_miss_filter")

  /**
    * An aggregation whose output LEFT joins probe, under a filter that drops the rows none of those
    * joins matched, filters its input rows by their builds first: a group whose key no build has is
    * dropped anyway. TPC-DS q78 keeps the (year, item, customer) groups of store_sales that web or
    * catalog sales have (`coalesce(ws_qty, 0) > 0 or coalesce(cs_qty, 0) > 0`), ~6K of its ~5M
    * input rows at SF10, so the aggregation and the joins see ~6K rows. The filter turns itself off
    * once it has seen 64K rows and kept more than half of them
    */
  case LeftMatchPrefilter extends Optimization("left_match_prefilter")

  /**
    * The one-Long entries of a flat join hash table, `(key - min) << rowBits | row`, give the row
    * only the bits the build rows need instead of 32, so keys spanning up to 64 - rowBits bits
    * still take one Long instead of two. TPC-DS q78's LEFT joins on (year, item, customer) pack
    * their keys into about 39 bits, which took 16-byte entries
    */
  case NarrowJoinEntries extends Optimization("narrow_join_entries")

  /**
    * A hash join over a single integer key whose build values span a dense range indexes its build
    * rows by `key - min` instead of hashing (DuckDB's perfect hash join)
    */
  case JoinDirectIndex extends Optimization("join_direct_index")

  /**
    * A large hash join build concatenates its input, finds its non-NULL keys and key ranges, and
    * fills its direct index in parallel tasks instead of on the calling thread, while the probe
    * pipeline waits for it
    */
  case ParallelJoinBuild extends Optimization("parallel_join_build")

  /**
    * With `parallelize`, the hash join builds a pipeline waits for run at the same time, each on a
    * coordinator thread outside the task pool, instead of one after another on the calling thread
    */
  case ConcurrentJoinBuilds extends Optimization("concurrent_join_builds")

  /**
    * Hash-join builds encode their string columns over their distinct values, so the probe outputs
    * carry codes that identify values across batches ([[wvlet.lang.engine.vector.StrVec.dict]]).
    * Aggregations whose string keys all carry such codes group rows by the packed codes (with the
    * other keys) in a table kept across batches, and hash and compare the strings once per group
    * (TPC-DS q47's item and store names)
    */
  case JoinBuildStringCodes extends Optimization("join_build_string_codes")

  /**
    * With `parallelize`, INTERSECT / EXCEPT (without ALL) run as semi / anti hash joins of the left
    * input's distinct rows with the right input on every column, NULLs equal, so that the probe
    * runs as morsels, as DuckDB plans them. Otherwise one set operation buffers the right input and
    * walks the left one on the calling thread
    */
  case SetOpJoins extends Optimization("set_op_joins")

  /**
    * A right / full join's unmatched build rows, emitted after every probe part, are emitted by
    * tasks over disjoint ranges of the build rows, each through its own copy of the operators above
    * the join, instead of in one run on the calling thread. Only when the pipeline has one right /
    * full join and the build has at least [[wvlet.lang.engine.exec.MorselOp.MinTailSplitRows]] rows
    */
  case ParallelOuterTail extends Optimization("parallel_outer_tail")

  /**
    * A NOT EXISTS and an EXISTS over the same table on the same key, decorrelated into an anti and
    * a semi join that both build on the outer rows, probe the table once: one RIGHT SEMI join marks
    * each build row matched by the EXISTS's rows and excluded by the NOT EXISTS's, and emits the
    * matched rows not excluded ([[wvlet.lang.engine.runtime.SemiAntiProbe]]). TPC-H q21 read
    * lineitem three times, once per join and once for its outer rows
    */
  case SemiAntiOneProbe extends Optimization("semi_anti_one_probe")

  /**
    * A join build's string columns give up their codes (`join_build_string_codes`) once their first
    * rows look nearly unique, and once one column gives up, the others aren't encoded. TPC-H q2's
    * supplier build hashed 10K rows of each of its four unique string columns on one thread before
    * keeping none of their codes
    */
  case StringCodeProbe extends Optimization("string_code_probe")

  /**
    * A large hash join build (`parallel_join_build`) also takes its key ranges, packs its
    * multi-column keys, hashes and partitions its flat table's rows, and finds the flat table's key
    * range and fills its empty entries in tasks instead of on the calling thread. TPC-DS q97's FULL
    * join over 2.9M (customer, item) keys ran these steps on one thread while the others waited
    */
  case ParallelJoinKeys extends Optimization("parallel_join_keys")

  /**
    * A pipeline whose stream is one in-memory batch (an aggregation's output or a CTE result) runs
    * as tasks over row ranges of the batch when its hash joins probe large builds, instead of on
    * the calling thread. TPC-DS q65 probed the 2.3M-row (store, item) revenue result with its 102
    * per-store averages on one thread, emitting every revenue row, while the workers waited
    */
  case SliceSingleBatchProbes extends Optimization("slice_single_batch_probes")

  // --- Physical: aggregation -------------------------------------------------------------------

  /**
    * Pre-size the group tables and aggregate state of hash aggregations from estimated group
    * counts, so they grow less often
    */
  case PresizeAggregates extends Optimization("presize_aggregates")

  /**
    * With `parallelize`, partitioned aggregations route their input rows to key partitions as
    * tasks, each hashing a run of input batches, instead of on the calling thread
    */
  case ParallelAggPartitioning extends Optimization("parallel_agg_partitioning")

  /**
    * Two-phase aggregation computes identical partial aggregates once: `avg(x)` (a sum and a count)
    * reuses the partials of `sum(x)` and `count(x)` in the same aggregation
    */
  case SharedAggPartials extends Optimization("shared_agg_partials")

  /**
    * The partial aggregates of a two-phase aggregation stop grouping once they keep nearly every
    * row as its own group, and pass the rows to the final aggregate as one-row partial results
    */
  case AdaptivePartialAggs extends Optimization("adaptive_partial_aggs")

  /**
    * Partitioned aggregations by several integer or date keys group each partition by one packed
    * Int64 key, from the key ranges of the partition's rows
    */
  case PackAggKeys extends Optimization("pack_agg_keys")

  /**
    * Streaming aggregations and DISTINCTs by several integer, date, or decimal keys group by one
    * packed Int64 key, from the key ranges seen so far, repacking their groups when a key falls
    * outside them
    */
  case StreamPackAggKeys extends Optimization("stream_pack_agg_keys")

  /**
    * Decimal grouping keys are hashed and compared as their unscaled Long values instead of boxed
    * BigDecimals, and decimals of precision up to 18 can be packed with other keys
    */
  case LongDecimalKeys extends Optimization("long_decimal_keys")

  /**
    * Parquet scans keep the dictionary codes of the strings they decode from a dictionary page
    * ([[wvlet.lang.engine.vector.StrVec.codes]]), and aggregations whose keys all carry codes look
    * up each code combination once per batch instead of hashing and comparing every row's strings
    * (DuckDB's dictionary aggregation)
    */
  case DictionaryGroupKeys extends Optimization("dictionary_group_keys")

  /**
    * Hash aggregations fold their counts and decimal sums in one pass over each batch's rows, so
    * that the per-group updates of different aggregates overlap instead of each aggregate waiting
    * on its previous row's store (TPC-H q1 has nine such partial aggregates over four groups)
    */
  case FusedAccumulators extends Optimization("fused_accumulators")

  /**
    * An aggregate (sum, count, min, max, avg) of a CASE of one WHEN without ELSE aggregates its
    * THEN value with the condition as its FILTER, so the rows the condition drops aren't folded
    * (TPC-DS q02, q59, q43: `sum(case when d_day_name = '...' then sales_price else null end)`)
    */
  case CaseAggFilters extends Optimization("case_agg_filters")

  /**
    * Aggregate FILTER conditions over one string column run once per dictionary entry. When the
    * column holds a hash-join build's codes (`join_build_string_codes`), the results over that
    * build's dictionary are kept across batches, and each row takes its code's result (TPC-DS q59
    * and q43's seven `filter (where d_day_name = '...')` over the date_dim build), instead of
    * comparing each row's string ([[wvlet.lang.engine.expr.PerSharedDictEntry]])
    */
  case DictionaryAggFilters extends Optimization("dictionary_agg_filters")

  /**
    * Several aggregate FILTERs over one string column holding a hash-join build's codes, which no
    * code passes twice (`dictionary_agg_filters`), route each row to the aggregate its code passes
    * in one pass over the codes, instead of each FILTER making its own mask, count and gather
    * (TPC-DS q59, q43 and q02's seven `filter (where d_day_name = '...')`,
    * [[wvlet.lang.engine.exec.RoutedAggFilters]])
    */
  case RoutedAggFilters extends Optimization("routed_agg_filters")

  /**
    * Partial aggregates that stop grouping ([[AdaptivePartialAggs]]) emit each row's partial
    * results from the aggregates' arguments (a sum's value, a count's 0 or 1) instead of folding
    * every passed-through batch into new accumulators with one group per row (TPC-DS q78, q97)
    */
  case OneRowPartials extends Optimization("one_row_partials")

  /**
    * Hash aggregations read the deferred columns of a filter's or join probe's output at their
    * selected positions instead of gathering them first: aggregates over a column (sum, min, max,
    * count of Int64, Double, and Long-backed decimals), `+` / `-` arguments over such columns (one
    * output array), and packed group keys ([[wvlet.lang.engine.exec.SelectedInputs]]). TPC-DS
    * q04/q11 spent about 11% of their CPU gathering sales columns for `sum(a - b)` and its keys
    */
  case AggregateSelectedRows extends Optimization("aggregate_selected_rows")

  /**
    * Hash aggregations over dense selected rows ([[DenseSelectedRows]]) compute fused decimal
    * products into Long arrays reused from batch to batch, and look the group ids of one or two
    * string keys up from their dictionary codes in one pass into a reused id array
    * ([[wvlet.lang.engine.expr.LongScratch]], [[wvlet.lang.engine.exec.DictionaryIdsAt]]). TPC-H q1
    * spent about 2% of its CPU zeroing fresh product arrays and 5% combining the codes of its two
    * keys column by column
    */
  case AggScratchBuffers extends Optimization("agg_scratch_buffers")

  /**
    * Hash aggregations group a single Int64 key, such as the packed key of several integer keys,
    * through a table of 4-byte group-id slots, half a GroupTable slot, comparing keys from the
    * groups' key array ([[wvlet.lang.engine.exec.LongGroupTable]]). The final stages of TPC-DS
    * q97's near-unique (customer, item) groups spent about 5% of their CPU assigning group ids
    */
  case LongGroupTables extends Optimization("long_group_tables")

  /**
    * Hash aggregations over one Int64-backed key group runs of equal keys without hashing them
    * while the keys come in non-decreasing order, falling back to hashing at the first key out of
    * order ([[wvlet.lang.engine.exec.LongGroupTable]]). Partial aggregations over a scan sorted by
    * the key (TPC-H lineitem by `l_orderkey`) and their final partitions, which get the partials in
    * part order, then assign group ids with one comparison per row
    */
  case SortedGroupRuns extends Optimization("sorted_group_runs")

  /**
    * Partitioned aggregations whose input comes in non-decreasing order of an integer grouping key
    * split it at key changes into contiguous runs, aggregated in parallel as the partitions,
    * instead of hashing and scattering every row ([[wvlet.lang.engine.exec.SortedAggSplits]]). The
    * partials of a scan sorted by the key (TPC-H lineitem by `l_orderkey`) come in part order, so
    * only the parts' boundary keys repeat
    */
  case SortedAggSplits extends Optimization("sorted_agg_splits")

  /**
    * UNION (distinct) runs one two-phase distinct over the union of its inputs' pipelines, with a
    * partial distinct per scan part and one final distinct partitioned by key, as DuckDB does,
    * instead of a full distinct of each input before the final one. Nested UNION (distinct) inputs
    * join the outer union, as `distinct(a ∪ distinct(b ∪ c)) = distinct(a ∪ b ∪ c)`. TPC-DS q75's
    * store and web sales rows were reduced to their distinct rows three times
    */
  case UnionDistinctOnce extends Optimization("union_distinct_once")

  /**
    * The fused fold of `fused_accumulators` adds one sum at a time over the batch, into four
    * partial sums per group that consecutive rows rotate over, with one overflow check per group
    * when the values' magnitudes bound the batch's sum, instead of updating every accumulator per
    * row with a per-row overflow check (TPC-H q1's five decimal sums over four groups)
    */
  case ColumnMajorFusedSums extends Optimization("column_major_fused_sums")

  /**
    * Partitioned aggregations whose keys pack into one Int64
    * ([[wvlet.lang.engine.exec.AggKeyPacking]]) pack them while routing rows to the key partitions,
    * from the keys' ranges over the whole input, and copy only the packed key and the aggregates'
    * columns into the partitions. Copying every column of the partitions' rows was ~2% of TPC-DS
    * q97's and q78's CPU at SF10, and each partition packed its keys again from its own ranges
    */
  case PackedAggRouting extends Optimization("packed_agg_routing")

  // --- Physical: windows -----------------------------------------------------------------------

  /**
    * A filter bounding a row_number() or rank() by a constant (`rk <= 100`) over a window of only
    * such calls keeps the top rows of each partition with a bounded heap instead of sorting the
    * whole partition
    */
  case WindowTopN extends Optimization("window_top_n")

  /**
    * With `window_top_n`, the window keeps the top rows of each partition within every chunk of
    * input batches as it reads them, in parallel tasks, instead of materializing its whole input
    * first; only the chunks' top rows are concatenated and ranked
    */
  case WindowTopNStream extends Optimization("window_top_n_stream")

  /**
    * Running window counts, sums, mins, and maxes (frames from the partition start) over primitive
    * columns are folded in primitive values, instead of through an accumulator and a result vector
    * per row
    */
  case RunningWindowKernels extends Optimization("running_window_kernels")

  /**
    * Window partitions ordered by one integer or date key are sorted as packed (key, position)
    * longs instead of through a row comparator
    */
  case WindowSortKernels extends Optimization("window_sort_kernels")

  /**
    * Window calls with the same PARTITION BY and ORDER BY are partitioned and sorted once, and
    * every call reads the same sorted partitions
    */
  case WindowSharePartitions extends Optimization("window_share_partitions")

  // --- Physical: expressions and deferred columns ----------------------------------------------

  /**
    * Filters and hash joins defer their output columns (row positions over their input columns)
    * instead of copying every column: each column is gathered once, by the operator that reads it
    */
  case LateGather extends Optimization("late_gather")

  /**
    * A non-trivial subexpression repeated across the expressions of a projection, filter,
    * aggregation, or window is computed once, in a projection below it
    * ([[wvlet.lang.engine.runtime.CommonSubexpressions]]), as DuckDB's and DataFusion's common
    * subexpression elimination do
    */
  case CommonSubexpressions extends Optimization("common_subexpressions")

  /**
    * Row-independent subexpressions are evaluated once when lowering, and integral constants added
    * to or subtracted from decimals become decimals at the result scale
    * ([[wvlet.lang.engine.runtime.FoldConstants]])
    */
  case FoldConstants extends Optimization("fold_constants")

  /**
    * A CASE of one WHEN without ELSE (or ELSE NULL) is its THEN vector with NULL where the
    * condition isn't true, sharing the THEN values, instead of a per-row branch choice that copies
    * them (TPC-DS q02's seven `sum(case when d_day_name = '...' then sales_price end)` inputs)
    */
  case MaskedCaseWhen extends Optimization("masked_case_when")

  /**
    * A CASE of one WHEN whose THEN and ELSE are non-NULL constants picks one of the two per row
    * from the condition, instead of the per-row branch choice (TPC-DS q97's `sum(case when c then 1
    * else 0 end)`)
    */
  case ConstCaseSelect extends Optimization("const_case_select")

  /**
    * Projections and hash aggregations over the deferred columns of a filter that keeps most rows
    * compute over the filter's input rows instead of gathering the kept ones: column references
    * pass through deferred, decimal and double `+` / `-` / `*` compute over every source row when
    * at least half are kept, the fused accumulators read their arguments at the kept positions, and
    * string group keys look up their dictionary codes there
    * ([[wvlet.lang.engine.exec.DenseSelectedRows]]). TPC-H q1 spent about 14% of its CPU gathering
    * the 98% of lineitem rows its date filter keeps
    */
  case DenseSelectedRows extends Optimization("dense_selected_rows")

  /**
    * Decimal `x * (c ± y)` for a constant c computes `c ± y` and the product per row in one loop
    * over unscaled Longs, with an overflow check that divides only for operands outside the Int
    * range, instead of an intermediate array and a Long division per row (TPC-H q1's
    * `l_extendedprice * (1 - l_discount) * (1 + l_tax)`, q3/q5/q10's revenue)
    */
  case FusedDecimalArith extends Optimization("fused_decimal_arith")

  /**
    * Common subexpression elimination counts subexpressions by a structural hash computed once per
    * node, bottom-up (`cse_hashed_keys`), instead of hashing each subtree again at every ancestor.
    * TPC-DS q41's filter, an OR of eight ANDs, spent ~20% of its planning hashing them
    */
  case CseHashedKeys extends Optimization("cse_hashed_keys")

  // --- Physical: parallel execution ------------------------------------------------------------

  /** Parallel scan pipelines and two-phase / partitioned aggregation */
  case Parallelize extends Optimization("parallelize")

  /**
    * With `parallelize`, read the output of a grouped aggregation as a source split by batch, so
    * that the filters, projections, join probes, and partial aggregations above it run as morsels
    * instead of on the calling thread
    */
  case ParallelAboveBreakers extends Optimization("parallel_above_breakers")

  /**
    * Run the plan subtrees that the engine's [[wvlet.lang.engine.runtime.PlanAccelerator]] (e.g. a
    * GPU) supports on it. Without an accelerator, nothing changes
    */
  case GpuOffload extends Optimization("gpu_offload")

  /**
    * With `parallelize`, a UNION ALL of scan pipelines is one pipeline: the parts of every input,
    * in input order, run through the operators above the union (hash join probes over one shared
    * build, partial aggregates) as morsels. Otherwise those operators run on the calling thread
    * over the union's rows (TPC-DS q02, q05, q76, q77, q80)
    */
  case UnionAllPipelines extends Optimization("union_all_pipelines")

  /**
    * A partition of a partitioned aggregation whose group estimate reached
    * [[wvlet.lang.engine.exec.GroupTable.MaxPresizeGroups]] sizes its group table and aggregate
    * state for its row count (an upper bound on its groups, up to that cap), instead of an even
    * share of the capped estimate. TPC-DS q97's and q78's final partitions hold ~50K groups each at
    * SF10 but were sized for 4096, and their rehashes and state growth were ~30% of the final
    * stage's CPU
    */
  case SizePartitionsByRows extends Optimization("size_partitions_by_rows")

  // --- Execution: resource accounting ---------------------------------------------------------

  /**
    * Count the bytes of each query's large in-memory structures (join builds, buffered results)
    * against its [[wvlet.lang.engine.exec.MemoryContext]] and the engine's
    * [[wvlet.lang.engine.exec.MemoryPool]], failing a query past its limit with RESOURCE_EXHAUSTED.
    * Off, nothing is counted and no limit applies
    */
  case MemoryAccounting extends Optimization("memory_accounting")

  /**
    * Each query's pool tasks form a [[wvlet.lang.engine.exec.TaskGroup]], and the engine's thread
    * pool picks tasks across queries by a multilevel feedback queue on their run time
    * ([[wvlet.lang.engine.exec.FairTaskQueue]]) instead of one FIFO queue, so a short query running
    * next to a long one isn't queued behind the long query's morsels. Changes no plan
    */
  case FairTaskScheduling extends Optimization("fair_task_scheduling")

  /**
    * With `fair_task_scheduling`, a query's coordinator tasks run on a second pool that picks them
    * fairly across queries by their own run time, instead of one FIFO queue. With or without it, a
    * query computing its shared CTE results at plan time computes those no coordinator has started
    * on its own thread instead of waiting for a coordinator. Changes no plan
    */
  case FairCoordinators extends Optimization("fair_coordinators")

  /**
    * A partitioned aggregation whose input would take the query past its spill threshold
    * (`WvletEngine.withSpillThreshold`, by default half the JVM's max heap) splits the input by key
    * hash into spill files and aggregates them in waves that fit, instead of holding it all
    * ([[wvlet.lang.engine.exec.AggSpill]]). Changes no plan, and nothing when the input fits
    */
  case SpillAggregation extends Optimization("spill_aggregation")

  /**
    * A hash join build whose input would take the query past its spill threshold splits its rows by
    * key hash into partitions, keeps those that fit, and writes the rest to spill files; probe rows
    * of the written partitions go to files too, and each written partition is joined on its own
    * after the probe input ends (grace hash join, [[wvlet.lang.engine.exec.JoinSpill]]). Changes no
    * plan, and nothing when the build fits
    */
  case SpillJoin extends Optimization("spill_join")

  /**
    * With `memory_accounting`, a shared CTE result (or grouping-set input) drops its batches and
    * returns its reservation once every scan that reads it in the planned query has finished,
    * instead of when the query ends, so a query holds only the results still being read. Counted
    * from the final operator tree; a reader that stops early (a limit) keeps the result. Changes no
    * plan
    */
  case ReleaseCTEResults extends Optimization("release_cte_results")

end Optimization

object Optimization:
  def fromName(name: String): Option[Optimization] = values.find(_.name == name)

/**
  * Which planner optimizations run. `validatePlans` checks every runtime plan with
  * [[wvlet.lang.engine.runtime.PlanValidator]] (for tests and debugging)
  */
case class OptimizerOptions(
    disabled: Set[Optimization] = Set.empty,
    validatePlans: Boolean = false
):
  def isEnabled(o: Optimization): Boolean = !disabled.contains(o)

  def withDisabled(o: Optimization*): OptimizerOptions = copy(disabled = disabled ++ o)
  def withEnabled(o: Optimization*): OptimizerOptions  = copy(disabled = disabled -- o)
  def noDisabled(): OptimizerOptions                   = copy(disabled = Set.empty)
  def withValidatePlans(b: Boolean): OptimizerOptions  = copy(validatePlans = b)

object OptimizerOptions:
  val PropertyName: String = "wvlet.engine.optimizer.disabled"
  val EnvName: String      = "WVLET_ENGINE_OPTIMIZER_DISABLED"

  /** Parse comma-separated optimization names. Unknown names are an error */
  def parse(names: String): OptimizerOptions =
    val disabled = names
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { n =>
        Optimization
          .fromName(n)
          .getOrElse {
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Unknown optimization in ${PropertyName}: ${n} (available: ${Optimization
                    .values
                    .map(_.name)
                    .mkString(", ")})"
              )
          }
      }
    OptimizerOptions(disabled.toSet)

  /**
    * The options set by the wvlet.engine.optimizer.disabled system property or the
    * WVLET_ENGINE_OPTIMIZER_DISABLED environment variable (every optimization runs when neither is
    * set)
    */
  def fromSystem(): OptimizerOptions = sys
    .props
    .get(PropertyName)
    .orElse(sys.env.get(EnvName))
    .map(parse)
    .getOrElse(OptimizerOptions())

end OptimizerOptions
