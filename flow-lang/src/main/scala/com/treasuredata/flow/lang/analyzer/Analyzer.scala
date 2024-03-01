package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.model.plan.FlowPlan
import com.treasuredata.flow.lang.parser.FlowParser
import wvlet.log.LogSupport

/**
  */
object Analyzer extends LogSupport:

  def analyzeSourceFolder(path: String): Seq[FlowPlan] =
    val plans = FlowParser.parseSourceFolder(path)
    plans

  def analyzeSingle(compileUnit: CompileUnit): FlowPlan =
    val plan: FlowPlan = FlowParser.parse(compileUnit)
    analyzeSingle(plan)

  def analyzeSingle(plan: FlowPlan): FlowPlan =
    val context = AnalyzerContext(Scope.Global, plan.compileUnit)
    // Collect
    TypeResolver.resolveSchemaAndTypes(context, plan)

    // TODO
    val resolvedPlan: FlowPlan = plan
    resolvedPlan

//
//  def analyze(plan: LogicalPlan, database: String, catalog: Catalog): LogicalPlan =
//    if plan.resolved then plan
//    else
//      val analyzerContext =
//        AnalyzerContext(database = database, catalog = catalog, parentAttributes = Some(plan.outputAttributes))
//      trace(s"Unresolved plan:\n${plan.pp}")
//
//      val resolvedPlan = TypeResolver.resolve(analyzerContext, plan)
//      trace(s"Resolved plan:\n${resolvedPlan.pp}"c)
//
//      val optimizedPlan = Optimizer.optimizerRules.foldLeft(resolvedPlan) { (targetPlan, rule) =>
//        val r = rule.apply(analyzerContext)
//        // Recursively transform the tree
//        targetPlan.transform(r)
//      }
//
//      trace(s"Optimized plan:\n${optimizedPlan.pp}")
//      optimizedPlan
