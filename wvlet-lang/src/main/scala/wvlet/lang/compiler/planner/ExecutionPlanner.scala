package wvlet.lang.compiler.planner

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Phase
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

    // Collect a trailing chain of test relations (in source order) and the tested relation below it
    def collectTestChain(t: TestRelation): (List[TestRelation], Option[Relation]) =
      val tests                               = List.newBuilder[TestRelation]
      def iter(r: Relation): Option[Relation] =
        r match
          case tr: TestRelation =>
            val ret = iter(tr.child)
            tests += tr
            ret
          case other =>
            Some(other)
      val nonTestChild = iter(t)
      (tests.result(), nonTestChild)

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
          val (tests, nonTestChild) = collectTestChain(t)
          val plans                 = List.newBuilder[ExecutionPlan]
          // Execute the tested query only when its result is consumed: this test chain is the
          // selected execution target, or the tests will read the result in a debug-run.
          // Otherwise just recurse to plan nested test/debug statements
          nonTestChild.foreach { c =>
            plans += plan(c, evalQuery = evalQuery || context.isDebugRun)
          }
          if context.isDebugRun then
            plans ++= tests.map(ExecuteTest(_))
          ExecutionPlan(plans.result())
        case save: Save =>
          val queryPlan = plan(save.inputRelation, evalQuery = false)
          ExecuteSave(save, queryPlan)
        case q: Query =>
          val plans = List.newBuilder[ExecutionPlan]
          q.child match
            case t: TestRelation if evalQuery && context.isDebugRun =>
              // Trailing tests read this query's own result: plan nested test/debug statements
              // first, run the (test-stripped) query once, then evaluate the trailing tests
              // against that result — instead of executing the same query a second time
              val (tests, nonTestChild) = collectTestChain(t)
              nonTestChild.foreach { c =>
                plans += plan(c, evalQuery = false)
              }
              plans += queryExecutePlan(q)
              plans ++= tests.map(ExecuteTest(_))
            case _ =>
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
        case f: FlowDef if f eq targetPlan =>
          // A flow runs only when its definition is the directly selected target statement.
          // Flow definitions embedded in a compilation unit are declarations and do not run on
          // whole-file execution; they are triggered explicitly, by schedules, or by dependencies
          ExecuteFlow(f)
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
