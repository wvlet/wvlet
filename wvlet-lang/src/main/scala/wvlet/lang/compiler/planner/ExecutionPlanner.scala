package wvlet.lang.compiler.planner

import wvlet.lang.compiler.{CompilationUnit, Context, Phase}
import wvlet.lang.model.plan.*

object ExecutionPlanner extends Phase("execution-plan"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    if context.isContextCompilationUnit then
      unit.executionPlan = plan(unit, context)
    unit

  def plan(unit: CompilationUnit, context: Context): ExecutionPlan =
    def plan(l: LogicalPlan): ExecutionPlan =
      l match
        case p: PackageDef =>
          val plans = p
            .statements
            .map { stmt =>
              plan(stmt)
            }
            .filter(!_.isEmpty)
          if plans.isEmpty then
            ExecutionPlan.empty
          else
            ExecuteTasks(plans)
        case q: QueryStatement =>
          val plans = List.newBuilder[ExecutionPlan]
          q.child match
            case t: TestRelation =>
              // TODO Support in-query test
              // TODO Persistent test data to local file or table (for Trino)
              plans += ExecuteQuery(t.child)
              plans += ExecuteTest(t)
            case _ =>
              plans += ExecuteQuery(q)
          ExecutionPlan(plans.result())
        case e: Execute =>
          ExecuteCommand(e)
        case other =>
          trace(s"Unsupported logical plan: ${other}")
          ExecutionPlan.empty

    val executionPlan = plan(unit.resolvedPlan)
    trace(s"Execution plan:\n${executionPlan}")
    executionPlan

  end plan

end ExecutionPlanner
