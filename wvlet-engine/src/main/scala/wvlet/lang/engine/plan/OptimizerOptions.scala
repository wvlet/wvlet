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

  /** Cost-based order of inner joins; disabled, the inputs are joined in written order */
  case ReorderJoins extends Optimization("reorder_joins")

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
    * Pre-size the group tables and aggregate state of hash aggregations from estimated group
    * counts, so they grow less often
    */
  case PresizeAggregates extends Optimization("presize_aggregates")

  /** Parallel scan pipelines and two-phase / partitioned aggregation */
  case Parallelize extends Optimization("parallelize")

  /**
    * Filters and hash joins defer their output columns (row positions over their input columns)
    * instead of copying every column: each column is gathered once, by the operator that reads it
    */
  case LateGather extends Optimization("late_gather")

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
