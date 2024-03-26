package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, Scope}
import com.treasuredata.flow.lang.compiler.parser.FlowParser
import com.treasuredata.flow.lang.model.plan.{FlowPlan, LogicalPlan, Relation}
import wvlet.log.LogSupport

case class AnalysisResult(
    context: Context,
    plans: Seq[FlowPlan]
)

object TypeScan extends Phase("type-scan") with LogSupport:
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Pre-process to collect all schema and types
    TypeScanner.scanTypeDefs(unit.untypedPlan, context)
    // Post-process to resolve unresolved types
    TypeScanner.scanTypeDefs(unit.untypedPlan, context)
    unit

/**
  */
object Resolver extends Phase("resolve-types") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    trace(context.scope.getAllTypes.map(t => s"[${t._1}]: ${t._2}").mkString("\n"))

    // resolve plans
    var resolvedPlan: FlowPlan = analyzeSingle(unit.untypedPlan, context)
    // resolve again to resolve unresolved relation types
    resolvedPlan = analyzeSingle(resolvedPlan, context)
    unit.typedPlan = resolvedPlan
    unit

  def analyzeSingle(plan: FlowPlan, context: Context): FlowPlan =
    val resolvedPlan: Seq[LogicalPlan] = plan.logicalPlans.map { p =>
      TypeResolver.resolve(p, context)
    }
    FlowPlan(resolvedPlan, plan.compileUnit)
