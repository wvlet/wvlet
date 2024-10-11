package wvlet.lang.compiler.planner

import wvlet.lang.compiler.{CompilationUnit, Context, Phase}
import wvlet.lang.model.plan.*

object ExecutionPlanner extends Phase("execution-plan"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    if context.isContextCompilationUnit then
      unit.executionPlan = plan(unit, context)
    unit

  def plan(unit: CompilationUnit, context: Context): ExecutionPlan =
    def plan(l: LogicalPlan, evalQuery: Boolean): ExecutionPlan =
      l match
        case p: PackageDef =>
          val plans = p
            .statements
            .map { stmt =>
              plan(stmt, evalQuery)
            }
            .filter(!_.isEmpty)
          ExecutionPlan(plans)
        case q: Query =>
          // Skip the top-level query wrapping
          plan(q.child, evalQuery)
        case d: Debug =>
          val debugPlan = plan(d.debugExpr, evalQuery = true)
          ExecuteDebug(d, debugPlan)
        case t: TestRelation =>
          val plans = List.newBuilder[ExecutionPlan]
          val tests = List.newBuilder[TestRelation]

          def findNonTestRel(r: Relation): Option[Relation] =
            r match
              case tr: TestRelation =>
                val ret = findNonTestRel(tr.child)
                tests += tr
                ret
              case other =>
                Some(other)
          val nonTestChild = findNonTestRel(t.child)
          tests += t

          // For evaluating the test, need to evaluate the sub query
          nonTestChild.foreach { c =>
            plans += plan(c, evalQuery = true)
          }
          plans ++= tests.result().map(ExecuteTest(_))
          ExecutionPlan(plans.result())
        case save: Save =>
          val queryPlan = plan(save.inputRelation, evalQuery = false)
          ExecuteSave(save, queryPlan)
        case d: DeleteOps =>
          val queryPlan = plan(d.inputRelation, evalQuery = false)
          ExecuteDelete(d, queryPlan)
        case r: Relation =>
          val plans = List.newBuilder[ExecutionPlan]
          // Iterate through the children to find any test/debug queries
          r.children
            .map { child =>
              plans += plan(child, evalQuery = false)
            }
          if evalQuery then
            plans += ExecuteQuery(r)
          ExecutionPlan(plans.result())
        case e: Execute =>
          ExecuteCommand(e)
        case v: ValDef =>
          ExecuteValDef(v)
        case other =>
          if context.isContextCompilationUnit then
            warn(s"Unsupported logical plan: ${other}")
          ExecutionPlan.empty

    val executionPlan = plan(unit.resolvedPlan, evalQuery = true)
    trace(s"[Logical plan]:\n${unit.resolvedPlan.pp})")
    debug(s"[Execution plan]:\n${executionPlan.pp}")
    executionPlan

  end plan

end ExecutionPlanner
