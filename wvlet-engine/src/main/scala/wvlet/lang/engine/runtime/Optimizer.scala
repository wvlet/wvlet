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
package wvlet.lang.engine.runtime

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.log.LogSupport

import scala.collection.mutable

/**
  * A local rewrite of one [[RuntimePlan]] node. Rules are named by an [[Optimization]], so that
  * `wvlet.engine.optimizer.disabled` turns them off
  */
trait Rule:
  def optimization: Optimization
  def name: String = optimization.name

  /** The rewritten node, or None when the rule doesn't apply. A rewrite keeps the output columns */
  def apply(p: RuntimePlan): Option[RuntimePlan]

/** A step of the [[Optimizer]] pipeline */
trait Pass:
  def name: String
  def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan

/**
  * The options of an optimizer run, and the rules that fired so far. `replanSemiJoinBuilds` is set
  * for the run that re-plans a query once its shared CTEs are computed: [[ReorderJoins]] then also
  * chooses again the build side of the semi and anti joins that read one. `newColumnId` gives ids
  * of new columns, unique within the query (the binder's); passes that add columns don't fire
  * without it
  */
class OptimizerContext(
    val options: OptimizerOptions,
    val replanSemiJoinBuilds: Boolean = false,
    val newColumnId: Option[() => ColumnId] = None
):
  private val counts = mutable.LinkedHashMap.empty[String, Int]

  def isEnabled(r: Rule): Boolean = options.isEnabled(r.optimization)

  /** Record that a rule rewrote a node */
  def fired(r: Rule): Unit = fired(r.name)

  /** Record that a rule or pass, by name, rewrote a plan */
  def fired(name: String): Unit = counts(name) = counts.getOrElse(name, 0) + 1

  /** How many times each rule fired, by rule name */
  def firedRules: Map[String, Int] = counts.toMap

/**
  * Local rules applied bottom-up until no rule fires. Hitting `maxIterations` means the rules
  * oscillate: an error when plans are validated (tests), a warning otherwise
  */
class RuleBatch(val name: String, val rules: Seq[Rule], maxIterations: Int = 10)
    extends Pass
    with LogSupport:

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    val active = rules.filter(ctx.isEnabled)
    if active.isEmpty then
      p
    else
      var plan      = p
      var changed   = true
      var iteration = 0
      while changed && iteration < maxIterations do
        changed = false
        plan = sweep(plan, active, ctx, () => changed = true)
        iteration += 1
      if changed then
        val msg = s"Rule batch ${name} didn't converge in ${maxIterations} iterations"
        if ctx.options.validatePlans then
          throw StatusCode.UNEXPECTED_STATE.newException(s"${msg}:\n${plan.explain}")
        else
          warn(msg)
      plan

  private def sweep(
      p: RuntimePlan,
      rules: Seq[Rule],
      ctx: OptimizerContext,
      onChange: () => Unit
  ): RuntimePlan =
    var node = p.withChildren(p.children.map(sweep(_, rules, ctx, onChange)))
    rules.foreach { r =>
      r(node).foreach { rewritten =>
        ctx.fired(r)
        onChange()
        node = rewritten
      }
    }
    node

end RuleBatch

/**
  * The logical rewrites of a [[RuntimePlan]], as an ordered list of passes. With `validatePlans`,
  * [[PlanValidator]] checks the plan after each pass. The rules that fired accumulate over runs in
  * [[firedRules]]
  */
class Optimizer(
    val passes: Seq[Pass],
    options: OptimizerOptions,
    replanSemiJoinBuilds: Boolean = false,
    newColumnId: Option[() => ColumnId] = None
):
  private val ctx = OptimizerContext(options, replanSemiJoinBuilds, newColumnId)

  def run(p: RuntimePlan): RuntimePlan =
    passes.foldLeft(p)((plan, pass) => validated(plan, pass.run(plan, ctx)))

  /** How many times each rule fired, by rule name */
  def firedRules: Map[String, Int] = ctx.firedRules

  /** `next`, the rewrite of `p`, checked with [[PlanValidator]] when it changed */
  private def validated(p: RuntimePlan, next: RuntimePlan): RuntimePlan =
    if options.validatePlans && (next ne p) then
      PlanValidator.validate(next)
    next

  /**
    * The passes run again over a shared CTE body once its readers' filters are pushed into it,
    * because their decisions read estimates the filters change: the build sides of its outer joins
    * ([[outerBuilds]]), then its aggregations computed below joins ([[eagerAggregation]]). The root
    * run already ran them over the body without the filters. A pass whose decision reads such
    * estimates is added here
    */
  def afterReaderFilters(p: RuntimePlan): RuntimePlan = eagerAggregation(outerBuilds(p))

  /** The plan pruned to the columns needed to output `needed` ([[PruneColumns.prune]]) */
  def pruneColumns(p: RuntimePlan, needed: Set[ColumnId]): RuntimePlan = validated(
    p,
    PruneColumns.prune(p, needed, ctx)
  )

  /**
    * A shared CTE body whose readers read only its distinct rows of the columns `needed`, with its
    * joins reduced ([[ReduceSemiJoins.reduceBody]])
    */
  def reduceSemiJoins(p: RuntimePlan, needed: Set[ColumnId]): RuntimePlan = validated(
    p,
    ReduceSemiJoins.reduceBody(p, needed, ctx)
  )

  /**
    * The plan with its aggregations computed eagerly below joins ([[EagerAggregation]]), for a
    * shared CTE body once its readers' filters are pushed into it
    */
  def eagerAggregation(p: RuntimePlan): RuntimePlan = validated(p, EagerAggregation.run(p, ctx))

  /**
    * The plan with the build sides of its outer joins chosen again ([[ReorderJoins.outerBuilds]]),
    * for a shared CTE body once its readers' filters are pushed into it; only through
    * [[afterReaderFilters]]
    */
  private def outerBuilds(p: RuntimePlan): RuntimePlan = validated(
    p,
    ReorderJoins.outerBuilds(p, ctx)
  )

  /** The plan with the row-group filters of its scans recomputed ([[PushScanFilters]]) */
  def pushScanFilters(p: RuntimePlan): RuntimePlan = validated(p, PushScanFilters.run(p, ctx))

  /**
    * The optimized plan with the conditions pushed into it, through its filters too
    * ([[PushDownFilters.filter]])
    */
  def pushFilters(p: RuntimePlan, conds: Seq[JoinCondition]): RuntimePlan = validated(
    p,
    PushDownFilters.filter(p, conds, ctx, throughFilters = true)
  )

end Optimizer

object Optimizer:
  /** The passes, in order */
  val defaultPasses: Seq[Pass] = Seq(
    Normalize,
    PushDownFilters,
    ReduceSemiJoins,
    ReorderJoins,
    GroupKeySemiJoins,
    AggregateWindows,
    EagerAggregation,
    DependentGroupKeys,
    DimensionGroupIds,
    LateColumns,
    PruneColumns,
    PushScanFilters
  )

  /**
    * The default passes. With `replanSemiJoinBuilds`, for a re-plan over computed shared CTEs; with
    * `newColumnId`, the ids of new columns ([[OptimizerContext]])
    */
  def default(
      options: OptimizerOptions,
      replanSemiJoinBuilds: Boolean = false,
      newColumnId: Option[() => ColumnId] = None
  ): Optimizer = Optimizer(defaultPasses, options, replanSemiJoinBuilds, newColumnId)

  /** An optimizer running a single pass, for per-rule tests */
  def only(
      pass: Pass,
      options: OptimizerOptions = OptimizerOptions(),
      newColumnId: Option[() => ColumnId] = None
  ): Optimizer = Optimizer(Seq(pass), options, newColumnId = newColumnId)

end Optimizer
