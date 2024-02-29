package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.model.expr.Attribute
import com.treasuredata.flow.lang.model.plan.{FlowPlan, LogicalPlan}
import com.treasuredata.flow.lang.parser.{FlowLangParser, FlowParser}
import wvlet.log.LogSupport

/**
  */
object Analyzer extends LogSupport:
  def analyze(compileUnit: CompileUnit): FlowPlan =
    val code = compileUnit.readAsString
    trace(s"analyze:\n${code}")
    val plan: FlowPlan = FlowParser.parse(code)

    val context = AnalyzerContext(Scope.Global, compileUnit)
    TypeResolver.resolveSchemaAndTypes(context, plan)

    debug(context.getSchemas)
    debug(context.getTypes)

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
//      trace(s"Resolved plan:\n${resolvedPlan.pp}")
//
//      val optimizedPlan = Optimizer.optimizerRules.foldLeft(resolvedPlan) { (targetPlan, rule) =>
//        val r = rule.apply(analyzerContext)
//        // Recursively transform the tree
//        targetPlan.transform(r)
//      }
//
//      trace(s"Optimized plan:\n${optimizedPlan.pp}")
//      optimizedPlan
