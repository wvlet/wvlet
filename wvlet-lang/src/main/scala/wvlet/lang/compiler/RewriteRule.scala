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
package wvlet.lang.compiler

import wvlet.lang.model.plan.{LogicalPlan, Relation}
import wvlet.log.{LogLevel, LogSupport, Logger}

import scala.util.control.NonFatal

object RewriteRule extends LogSupport:
  type PlanRewriter = PartialFunction[LogicalPlan, LogicalPlan]

  def rewriteRelation(plan: Relation, rules: List[RewriteRule], context: Context): Relation =
    rewrite(plan, rules, context) match
      case r: Relation =>
        r
      case other =>
        throw new IllegalStateException(s"Expected Relation but got ${other.modelName}")

  def rewrite(plan: LogicalPlan, rules: List[RewriteRule], context: Context): LogicalPlan =
    val rewrittenPlan =
      rules.foldLeft(plan) { (p, rule) =>
        try
          val rewritten = rule.transform(p, context)
          rewritten
        catch
          case NonFatal(e) =>
            debug(s"Failed to rewrite with: ${rule.name}\n${p.pp}")
            throw e
      }
    rewrittenPlan

trait RewriteRule extends LogSupport:
  // Prepare a stable logger for debugging purpose
  private val localLogger = Logger("wvlet.lang.compiler.RewriteRule")

  def name: String = this.getClass.getSimpleName.stripSuffix("$")

  /**
    * Override this rule to skip the rule for the given plan
    * @param plan
    * @param context
    * @return
    */
  def isTargetPlan(plan: LogicalPlan, context: Context): Boolean = true

  /**
    * Return a partial function to rewrite the plan with LogicalPlan.transformUp
    * @param context
    * @return
    */
  def apply(context: Context): RewriteRule.PlanRewriter

  /**
    * Override this rule to apply a rewrite rule after the transformation
    * @param plan
    * @param context
    * @return
    */
  def postProcess(plan: LogicalPlan, context: Context): LogicalPlan = plan

  def transform(plan: LogicalPlan, context: Context): LogicalPlan =
    if !isTargetPlan(plan, context) then
      plan
    else
      val rule = this.apply(context)
      // Recursively transform the tree form bottom to up
      val resolved = plan.transformUp(rule)
      if localLogger.isEnabled(LogLevel.TRACE) && !(plan eq resolved) && plan != resolved then
        if context.isContextCompilationUnit then
          localLogger
            .trace(s"Transformed with ${name}:\n[before]\n${plan.pp}\n[after]\n${resolved.pp}")

      // Apply post-process filter
      postProcess(resolved, context)

end RewriteRule
