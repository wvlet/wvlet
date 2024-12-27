package wvlet.lang.compiler.planner

import wvlet.lang.compiler.{Context, CompilationUnit, ContextLogSupport, Phase}
import wvlet.lang.model.plan.ExecutionPlan

trait ExecutionPlanRewriteRule:
  def rewrite(plan: ExecutionPlan, context: Context): ExecutionPlan

object ExecutionPlanRewriter extends Phase("exec-plan-rewriter") with ContextLogSupport:

  def defaultRules: List[ExecutionPlanRewriteRule] = Nil

  def run(unit: CompilationUnit, context: Context): CompilationUnit =
    context.logInfo(s"Rewriting execution plan (${unit.sourceFile}):\n${unit.executionPlan.pp} ")
    unit


  object RewriteSubscribeTableFunction extends ExecutionPlanRewriteRule:
    override def rewrite(plan: ExecutionPlan, context: Context): ExecutionPlan =
      plan
