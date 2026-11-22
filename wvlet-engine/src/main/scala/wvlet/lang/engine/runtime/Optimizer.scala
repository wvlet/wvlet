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

/** The options of an optimizer run, and the rules that fired so far */
class OptimizerContext(val options: OptimizerOptions):
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
class Optimizer(val passes: Seq[Pass], options: OptimizerOptions):
  private val ctx = OptimizerContext(options)

  def run(p: RuntimePlan): RuntimePlan =
    passes.foldLeft(p) { (plan, pass) =>
      val next = pass.run(plan, ctx)
      if options.validatePlans && (next ne plan) then
        PlanValidator.validate(next)
      next
    }

  /** How many times each rule fired, by rule name */
  def firedRules: Map[String, Int] = ctx.firedRules

object Optimizer:
  /** The passes, in order */
  val defaultPasses: Seq[Pass] = Seq(Normalize, ReorderJoins)

  /** The default passes */
  def default(options: OptimizerOptions): Optimizer = Optimizer(defaultPasses, options)

  /** An optimizer running a single pass, for per-rule tests */
  def only(pass: Pass, options: OptimizerOptions = OptimizerOptions()): Optimizer = Optimizer(
    Seq(pass),
    options
  )
