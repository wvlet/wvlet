package wvlet.lang.compiler.planner

import wvlet.lang.compiler.{CompilationUnit, Context, Phase}
import wvlet.lang.model.plan.*

object ExecutionPlanner extends Phase("execution-plan"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    if context.isContextCompilationUnit then
      unit.executionPlan = plan(unit, context)
    unit

  def plan(unit: CompilationUnit, context: Context): ExecutionPlan =
    plan(unit, unit.resolvedPlan)(using context)

  def plan(unit: CompilationUnit, targetPlan: LogicalPlan)(using context: Context): ExecutionPlan =

    def queryExecutePlan(r: Relation): ExecutionPlan =
      // Remove any test and debug expressions
      val queryWithoutTests = r.transformUp {
        case r: TestRelation =>
          r.child
        case d: Debug =>
          d.child
      }
      ExecuteQuery(queryWithoutTests)

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
          if context.isDebugRun then
            plans ++= tests.result().map(ExecuteTest(_))
          ExecutionPlan(plans.result())
        case save: Save =>
          val queryPlan = plan(save.inputRelation, evalQuery = false)
          ExecuteSave(save, queryPlan)
        case q: Query =>
          val plans = List.newBuilder[ExecutionPlan]
          if evalQuery then
            plans += queryExecutePlan(q)

          // Evaluate inner query, debug, and test expressions
          plans += plan(q.child, false)
          ExecutionPlan(plans.result())
        case r: Relation =>
          val plans = List.newBuilder[ExecutionPlan]
          // Iterate through the children to find any test/debug queries
          r match
            case w: WithQuery =>
              // WithQuery needs a specific tree traversal
              w.queryDefs
                .foreach { d =>
                  plans += plan(d, evalQuery = false)
                }
              plans += plan(w.queryBody, evalQuery = false)
            case other =>
              r.children
                .map { child =>
                  plans += plan(child, evalQuery = false)
                }
          if evalQuery then
            plans += queryExecutePlan(r)

          ExecutionPlan(plans.result())
        case c: Command =>
          ExecuteCommand(c)
        case v: ValDef =>
          ExecuteValDef(v)
        case other =>
          if context.isContextCompilationUnit then
            trace(s"Unsupported logical plan: ${other}")
          ExecutionPlan.empty

    val executionPlan = plan(targetPlan, evalQuery = true)
    trace(s"[Logical plan]:\n${targetPlan.pp}")
    debug(s"[Execution plan]:\n${executionPlan.pp}")
    executionPlan

  end plan

end ExecutionPlanner
