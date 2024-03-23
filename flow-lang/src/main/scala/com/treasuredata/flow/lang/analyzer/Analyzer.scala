package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.model.plan.{FlowPlan, LogicalPlan, Relation}
import com.treasuredata.flow.lang.parser.FlowParser
import wvlet.log.LogSupport

case class AnalysisResult(
    context: AnalyzerContext,
    plans: Seq[FlowPlan]
)

/**
  */
object Analyzer extends LogSupport:

  def analyzeSourceFolder(path: String): AnalysisResult =
    val plans   = FlowParser.parseSourceFolder(path)
    val context = AnalyzerContext(Scope.Global)

    def typeScan: Unit = for plan <- plans do
      context.withCompileUnit(plan.compileUnit) { context =>
        TypeScanner.scanTypeDefs(plan, context)
      }

    // Pre-process to collect all schema and typs
    typeScan

    // Post-process to resolve unresolved typesa
    typeScan

    trace(context.getTypes.map(t => s"[${t._1}]: ${t._2}").mkString("\n"))

    // resolve plans
    var resolvedPlans: Seq[FlowPlan] = for plan <- plans yield analyzeSingle(plan, context)
    // resolve again to resolve unresolved relation types
    resolvedPlans = for plan <- resolvedPlans yield analyzeSingle(plan, context)

    // Return the context and resolved plans
    AnalysisResult(
      context = context,
      plans = resolvedPlans
    )

  def analyzeSingle(plan: FlowPlan, globalContext: AnalyzerContext): FlowPlan =
    // Resolve schema and types first.
    globalContext.withCompileUnit(plan.compileUnit) { context =>
      // Then, resolve the plan
      val resolvedPlan: Seq[LogicalPlan] = plan.logicalPlans.map { p =>
        TypeResolver.resolve(p, context)
      }
      FlowPlan(resolvedPlan, plan.compileUnit)
    }
