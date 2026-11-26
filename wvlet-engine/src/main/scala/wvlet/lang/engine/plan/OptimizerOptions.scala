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
  * values of `wvlet.engine.optimizer.disabled`, and the optimizer passes reuse them
  */
enum Optimization(val name: String):
  /** `(a and b) or (a and c)` => `a and (b or c)` */
  case FactorOr extends Optimization("factor_or")

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
    * Plan ROLLUP / CUBE (grouping sets whose key union is one of the sets) by aggregating the input
    * once, by the finest set, into partial results, then each coarser set from the partial results
    * of its smallest finer set, instead of one aggregation over the whole input per set
    */
  case ChainGroupingSets extends Optimization("chain_grouping_sets")

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

  /** Evaluate uncorrelated scalar subqueries over the same input as one query */
  case PrefetchScalarSubqueries extends Optimization("prefetch_scalar_subqueries")

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

  /** Read and pass only the columns the operators above use */
  case PruneColumns extends Optimization("prune_columns")

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
    * Pre-size the group tables and aggregate state of hash aggregations from estimated group
    * counts, so they grow less often
    */
  case PresizeAggregates extends Optimization("presize_aggregates")

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

  /** Parallel scan pipelines and two-phase / partitioned aggregation */
  case Parallelize extends Optimization("parallelize")

  /**
    * Filters and hash joins defer their output columns (row positions over their input columns)
    * instead of copying every column: each column is gathered once, by the operator that reads it
    */
  case LateGather extends Optimization("late_gather")

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
    * With `parallelize`, compute the shared CTE results a plan reads at the same time, each drained
    * on a coordinator thread outside the task pool, instead of one by one on the calling thread
    */
  case ConcurrentSharedCTEs extends Optimization("concurrent_shared_ctes")

  /**
    * With `parallelize`, read the output of a grouped aggregation as a source split by batch, so
    * that the filters, projections, join probes, and partial aggregations above it run as morsels
    * instead of on the calling thread
    */
  case ParallelAboveBreakers extends Optimization("parallel_above_breakers")

  /**
    * With `parallelize`, partitioned aggregations route their input rows to key partitions as
    * tasks, each hashing a run of input batches, instead of on the calling thread
    */
  case ParallelAggPartitioning extends Optimization("parallel_agg_partitioning")

  /**
    * Run the plan subtrees that the engine's [[wvlet.lang.engine.runtime.PlanAccelerator]] (e.g. a
    * GPU) supports on it. Without an accelerator, nothing changes
    */
  case GpuOffload extends Optimization("gpu_offload")

  /**
    * Keep the parsed footers of local Parquet files across opens, keyed by path and checked against
    * the file's size and last-modified time ([[wvlet.lang.engine.catalog.ParquetFooterCache]]), so
    * that planning and each scan don't parse the same footer again (DuckDB's
    * `parquet_metadata_cache`)
    */
  case ParquetFooterCache extends Optimization("parquet_footer_cache")

  /**
    * A non-trivial subexpression repeated across the expressions of a projection, filter,
    * aggregation, or window is computed once, in a projection below it
    * ([[wvlet.lang.engine.runtime.CommonSubexpressions]]), as DuckDB's and DataFusion's common
    * subexpression elimination do
    */
  case CommonSubexpressions extends Optimization("common_subexpressions")

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
    * Hash-join builds encode their string columns over their distinct values, so the probe outputs
    * carry codes that identify values across batches ([[wvlet.lang.engine.vector.StrVec.dict]]).
    * Aggregations whose string keys all carry such codes group rows by the packed codes (with the
    * other keys) in a table kept across batches, and hash and compare the strings once per group
    * (TPC-DS q47's item and store names)
    */
  case JoinBuildStringCodes extends Optimization("join_build_string_codes")

  /**
    * Row-independent subexpressions are evaluated once when lowering, and integral constants added
    * to or subtracted from decimals become decimals at the result scale
    * ([[wvlet.lang.engine.runtime.FoldConstants]])
    */
  case FoldConstants extends Optimization("fold_constants")

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
    * With `parallelize`, INTERSECT / EXCEPT (without ALL) run as semi / anti hash joins of the left
    * input's distinct rows with the right input on every column, NULLs equal, so that the probe
    * runs as morsels, as DuckDB plans them. Otherwise one set operation buffers the right input and
    * walks the left one on the calling thread
    */
  case SetOpJoins extends Optimization("set_op_joins")

  /**
    * With `push_down_filters`, an `or` condition of an inner join tree that reads several inputs
    * also filters each input by the `or`, over its branches, of the conjuncts that read only that
    * input, when every branch has one (TPC-H q19's part filter, q7's nation names). The `or` stays,
    * so the rows are the same ([[PushDownFilters]])
    */
  case OrImpliedFilters extends Optimization("or_implied_filters")

  /**
    * With `parallelize`, a UNION ALL of scan pipelines is one pipeline: the parts of every input,
    * in input order, run through the operators above the union (hash join probes over one shared
    * build, partial aggregates) as morsels. Otherwise those operators run on the calling thread
    * over the union's rows (TPC-DS q02, q05, q76, q77, q80)
    */
  case UnionAllPipelines extends Optimization("union_all_pipelines")

  /**
    * With `swap_build_side`, a semi / anti join of an EXISTS / IN subquery builds on the outer
    * input (a RIGHT SEMI / RIGHT ANTI join) whenever the subquery input is estimated larger, not
    * only [[JoinOrderOptimizer.SwapBuildRatio]] times larger: a swapped semi join outputs only the
    * outer columns, so it needs no reordering projection, and its build keys filter the larger
    * probe scan (TPC-H q4, q21 build on orders instead of lineitem)
    */
  case SemiJoinBuildSide extends Optimization("semi_join_build_side")

  /**
    * With `push_scan_filters`, comparisons of decimal columns with constants, and `<`, `<=` and `=`
    * of floating-point columns, also skip Parquet row groups by their min/max statistics
    * ([[ScanFilters]]). Floating-point `>` and `>=` stay out: writers leave NaN out of the
    * statistics while the engine orders NaN above every number
    */
  case DecimalFloatScanFilters extends Optimization("decimal_float_scan_filters")

  /**
    * Estimate a range comparison of an integer or date column with a constant (`<`, `>`, `between`,
    * date ranges) from the column's [min, max] in the Parquet statistics, with the bounds on one
    * column combined into one interval ([[wvlet.lang.engine.runtime.RangeSelectivity]]), instead of
    * the default selectivity per comparison
    */
  case RangeSelectivity extends Optimization("range_selectivity")

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
    * Run [[EagerAggregation]] over the body of a shared input the planner builds while binding (the
    * finest aggregation of chained grouping sets ([[ChainGroupingSets]]), and the inputs of other
    * grouping sets and unpivots), which the runtime passes don't visit: TPC-DS q22's ROLLUP of 26M
    * inventory rows by four item strings sums them per item key before joining the 102K items
    */
  case EagerGroupingSets extends Optimization("eager_grouping_sets")

  /**
    * With [[EagerAggregation]], also aggregate one side of the join eagerly when the other calls
    * are mins and maxes of the other side's columns, which the outer aggregation computes over the
    * joined rows as before (duplicates don't change them), and when the other side is a cross join
    * with one row: TPC-DS q23's best customers sum 27M store sales per customer key before joining
    * the customers crossed with the scalar maximum they compare the sums to
    */
  case EagerOtherSideMinMax extends Optimization("eager_other_side_min_max")

  /**
    * Count the distinct values of an integer column of a shared CTE result computed for planning
    * ([[CTEReaderStats]]) in a set of longs sized for its rows instead of a growing group table:
    * the same counts, so the same plans, for less planning time (TPC-DS q01 counts 540K customer
    * keys when it orders its joins again at SF10)
    */
  case PrimitiveResultDistinct extends Optimization("primitive_result_distinct")

  /**
    * Hash aggregations fold their counts and decimal sums in one pass over each batch's rows, so
    * that the per-group updates of different aggregates overlap instead of each aggregate waiting
    * on its previous row's store (TPC-H q1 has nine such partial aggregates over four groups)
    */
  case FusedAccumulators extends Optimization("fused_accumulators")

  /**
    * A CASE of one WHEN without ELSE (or ELSE NULL) is its THEN vector with NULL where the
    * condition isn't true, sharing the THEN values, instead of a per-row branch choice that copies
    * them (TPC-DS q02's seven `sum(case when d_day_name = '...' then sales_price end)` inputs)
    */
  case MaskedCaseWhen extends Optimization("masked_case_when")

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
    * A CASE of one WHEN whose THEN and ELSE are non-NULL constants picks one of the two per row
    * from the condition, instead of the per-row branch choice (TPC-DS q97's `sum(case when c then 1
    * else 0 end)`)
    */
  case ConstCaseSelect extends Optimization("const_case_select")

  /**
    * An aggregate (sum, count, min, max, avg) of a CASE of one WHEN without ELSE aggregates its
    * THEN value with the condition as its FILTER, so the rows the condition drops aren't folded
    * (TPC-DS q02, q59, q43: `sum(case when d_day_name = '...' then sales_price else null end)`)
    */
  case CaseAggFilters extends Optimization("case_agg_filters")

  /**
    * Partial aggregates that stop grouping ([[AdaptivePartialAggs]]) emit each row's partial
    * results from the aggregates' arguments (a sum's value, a count's 0 or 1) instead of folding
    * every passed-through batch into new accumulators with one group per row (TPC-DS q78, q97)
    */
  case OneRowPartials extends Optimization("one_row_partials")

  /**
    * `substring(s, k, n) IN ('a', ...)` with constant `k >= 1` and `n` looks up each string's
    * region in a hash table of the values, without building the substrings (TPC-H q22's
    * `c_phone.substring(1, 2).in(...)`)
    */
  case SubstringInLists extends Optimization("substring_in_lists")

  /**
    * Estimate string `=`, IN, LIKE and IS NULL conditions (and AND / OR / NOT of them) over one
    * column of a Parquet file from the values of a few sampled row groups
    * ([[wvlet.lang.engine.catalog.ValueCounts]]), cached per file version, for the scan row-filter
    * gate (`selective_scan_row_filters`) and the join search, instead of 1 / ndv or the default 0.3
    * (TPC-H q3's `c_mktsegment = 'BUILDING'` keeps 20%)
    */
  case StringValueSelectivity extends Optimization("string_value_selectivity")

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
    * Hash aggregations read the deferred columns of a filter's or join probe's output at their
    * selected positions instead of gathering them first: aggregates over a column (sum, min, max,
    * count of Int64, Double, and Long-backed decimals), `+` / `-` arguments over such columns (one
    * output array), and packed group keys ([[wvlet.lang.engine.exec.SelectedInputs]]). TPC-DS
    * q04/q11 spent about 11% of their CPU gathering sales columns for `sum(a - b)` and its keys
    */
  case AggregateSelectedRows extends Optimization("aggregate_selected_rows")

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
