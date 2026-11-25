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
    * Enumerate the join trees of small inner join graphs exactly, including bushy trees, instead of
    * multi-start greedy search
    */
  case ExactJoinOrder extends Optimization("exact_join_order")

  /** Build the hash table on the smaller input of a join */
  case SwapBuildSide extends Optimization("swap_build_side")

  /** Evaluate uncorrelated scalar subqueries over the same input as one query */
  case PrefetchScalarSubqueries extends Optimization("prefetch_scalar_subqueries")

  /** Compute a CTE referenced more than once only once */
  case ShareCTEs extends Optimization("share_ctes")

  /**
    * Filter a shared CTE's body by what its readers' filters agree on, when every reader reports
    * one (with `push_down_filters`)
    */
  case PushDownCTEFilters extends Optimization("push_down_cte_filters")

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
