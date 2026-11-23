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
    * Push filter conjuncts down the runtime plan: through projections, aggregates (grouping keys
    * only), and sorts, into the preserved side of outer joins and into inner join trees, with
    * equalities to constants inferred transitively there
    */
  case PushDownFilters extends Optimization("push_down_filters")

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

  /** Read and pass only the columns the operators above use */
  case PruneColumns extends Optimization("prune_columns")

  /** Skip Parquet row groups by the filters directly over a scan */
  case PushScanFilters extends Optimization("push_scan_filters")

  /** Parallel scan pipelines and two-phase / partitioned aggregation */
  case Parallelize extends Optimization("parallelize")

end Optimization

object Optimization:
  def fromName(name: String): Option[Optimization] = values.find(_.name == name)

/** How relations are planned into operators. The names are the values of `wvlet.engine.planner` */
enum PlannerMode(val name: String):
  /** Directly by the recursive [[Planner]] */
  case Legacy extends PlannerMode("legacy")

  /**
    * Through the engine's runtime plan ([[wvlet.lang.engine.runtime.RuntimePlan]]): relations it
    * doesn't cover yet are planned by the recursive planner
    */
  case Runtime extends PlannerMode("runtime")

object PlannerMode:
  def fromName(name: String): Option[PlannerMode] = values.find(_.name == name)

/**
  * Which planner optimizations run, and how relations are planned. `validatePlans` checks every
  * runtime plan with [[wvlet.lang.engine.runtime.PlanValidator]] (for tests and debugging)
  */
case class OptimizerOptions(
    disabled: Set[Optimization] = Set.empty,
    planner: PlannerMode = PlannerMode.Runtime,
    validatePlans: Boolean = false
):
  def isEnabled(o: Optimization): Boolean = !disabled.contains(o)

  def withDisabled(o: Optimization*): OptimizerOptions = copy(disabled = disabled ++ o)
  def withEnabled(o: Optimization*): OptimizerOptions  = copy(disabled = disabled -- o)
  def noDisabled(): OptimizerOptions                   = copy(disabled = Set.empty)
  def withPlanner(p: PlannerMode): OptimizerOptions    = copy(planner = p)
  def withValidatePlans(b: Boolean): OptimizerOptions  = copy(validatePlans = b)

object OptimizerOptions:
  val PropertyName: String        = "wvlet.engine.optimizer.disabled"
  val EnvName: String             = "WVLET_ENGINE_OPTIMIZER_DISABLED"
  val PlannerPropertyName: String = "wvlet.engine.planner"
  val PlannerEnvName: String      = "WVLET_ENGINE_PLANNER"

  /** Parse a planner mode name (`legacy` or `runtime`). Unknown names are an error */
  def parsePlanner(name: String): PlannerMode = PlannerMode
    .fromName(name.trim.toLowerCase)
    .getOrElse {
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Unknown planner in ${PlannerPropertyName}: ${name} (available: ${PlannerMode
              .values
              .map(_.name)
              .mkString(", ")})"
        )
    }

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
    * set), and the planner set by wvlet.engine.planner or WVLET_ENGINE_PLANNER (runtime by default)
    */
  def fromSystem(): OptimizerOptions =
    val o = sys
      .props
      .get(PropertyName)
      .orElse(sys.env.get(EnvName))
      .map(parse)
      .getOrElse(OptimizerOptions())
    sys
      .props
      .get(PlannerPropertyName)
      .orElse(sys.env.get(PlannerEnvName))
      .map(n => o.withPlanner(parsePlanner(n)))
      .getOrElse(o)

end OptimizerOptions
