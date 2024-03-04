package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.log.{LogLevel, LogSupport, Logger}

object RewriteRule:
  type PlanRewriter = PartialFunction[LogicalPlan, LogicalPlan]

trait RewriteRule extends LogSupport:
  // Prepare a stable logger for debugging purpose
  private val localLogger = Logger("com.treasuredata.flow.lang.analyzer.RewriteRule")

  def name: String = this.getClass.getSimpleName.stripSuffix("$")
  def apply(context: AnalyzerContext): RewriteRule.PlanRewriter

  def transform(plan: LogicalPlan, context: AnalyzerContext): LogicalPlan =
    val rule = this.apply(context)
    // Recursively transform the tree form bottom to up
    val resolved = plan.transformUp(rule)
    if localLogger.isEnabled(LogLevel.TRACE) && !(plan eq resolved) && plan != resolved then
      localLogger.trace(s"transformed with ${name}:\n[before]\n${plan.pp}\n[after]\n${resolved.pp}")
    resolved
