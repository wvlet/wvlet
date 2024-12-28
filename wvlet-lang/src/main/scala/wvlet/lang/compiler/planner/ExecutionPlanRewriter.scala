package wvlet.lang.compiler.planner

import wvlet.lang.api.Span
import wvlet.lang.compiler.analyzer.TypeResolver
import wvlet.lang.compiler.{CompilationUnit, Context, ContextLogSupport, Phase}
import wvlet.lang.model.expr.{DotRef, Identifier, NameExpr, UnquotedIdentifier}
import wvlet.lang.model.plan.*

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

    private def hasSubscriptionCall(plan: LogicalPlan): Boolean =
      var found = false
      plan.traverseOnce {
        case t: TableFunctionCall if t.name.leafName == "subscribe" =>
          found = true
      }
      found

    override def rewrite(plan: ExecutionPlan, context: Context): ExecutionPlan = plan.transformUp {
      case q: ExecuteQuery if hasSubscriptionCall(q.plan) =>
        var subscriptionTarget: List[NameExpr] = List.empty

        val newQueryPlan = q
          .plan
          .transformOnce {
            case TableFunctionCall(DotRef(qual: Identifier, funName, _, _), args, span)
                if funName.fullName == "subscribe" =>
              subscriptionTarget ::= qual
              val savedModelName = s"__${qual.fullName}"
              TableRef(UnquotedIdentifier(savedModelName, span), span)
          }

        if subscriptionTarget == null then
          q
        else
          val newPlans = List.newBuilder[ExecutionPlan]
          subscriptionTarget.map { t =>
            // TODO Append time conditions for subscription
            val query = TableRef(t, Span.NoSpan)
            val resolvedQuery: Relation = TypeResolver
              .resolve(query, context)
              .asInstanceOf[Relation]
            val save: Save = SaveTo(
              resolvedQuery,
              UnquotedIdentifier(s"__${t.fullName}", Span.NoSpan),
              Nil,
              Span.NoSpan
            )
            newPlans += ExecuteSave(save, ExecuteQuery(resolvedQuery))
          }
          newPlans += ExecuteQuery(newQueryPlan)
          ExecuteTasks(tasks = newPlans.result())
    }

  end RewriteSubscribeTableFunction

end ExecutionPlanRewriter
