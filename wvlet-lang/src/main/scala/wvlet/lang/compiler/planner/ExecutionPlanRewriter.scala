package wvlet.lang.compiler.planner

import wvlet.lang.compiler.{CompilationUnit, Context, ContextLogSupport, Phase}
import wvlet.lang.model.plan.{ExecuteQuery, ExecutionPlan, TableFunctionCall}

trait ExecutionPlanRewriteRule:
  def rewrite(plan: ExecutionPlan, context: Context): ExecutionPlan

object ExecutionPlanRewriter extends Phase("exec-plan-rewriter") with ContextLogSupport:

  def defaultRules: List[ExecutionPlanRewriteRule] = RewriteSubscribeTableFunction :: Nil

  def run(unit: CompilationUnit, context: Context): CompilationUnit =
    context.logTrace(s"Rewriting execution plan (${unit.sourceFile}):\n${unit.executionPlan.pp} ")

    val newExecutionPlan =
      defaultRules.foldLeft(unit.executionPlan) { (p, r) =>
        r.rewrite(p, context)
      }

    if newExecutionPlan ne unit.executionPlan then
      context.logDebug(s"Rewritten execution plan (${unit.sourceFile}):\n${newExecutionPlan.pp} ")
      unit.executionPlan = newExecutionPlan

    unit

  object RewriteSubscribeTableFunction extends ExecutionPlanRewriteRule:
    override def rewrite(plan: ExecutionPlan, context: Context): ExecutionPlan = plan.transformUp {
      case q: ExecuteQuery =>
        q.plan
          .traverse {
            case t: TableFunctionCall if t.name.leafName == "subscribe" =>
              warn(s"Found a subscription query: ${t.pp}")
          }

        q
    }
