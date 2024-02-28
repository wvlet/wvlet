package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.model.expr.Attribute
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import com.treasuredata.flow.lang.parser.{FlowLangParser, FlowParser}
import wvlet.log.LogSupport

/**
  * Propagate context
  *
  * @param database
  *   context database
  * @param catalog
  * @param parentAttributes
  *   attributes used in the parent relation. This is used for pruning unnecessary columns output attributes
  */
case class AnalyzerContext(
    database: String,
    catalog: Catalog,
    parentAttributes: Option[Seq[Attribute]] = None,
    outerQueries: Map[String, LogicalPlan] = Map.empty
):

  /**
    * Update the relation attributes used in the plan.
    *
    * @param parentAttributes
    * @return
    */
  def withAttributes(parentAttributes: Seq[Attribute]): AnalyzerContext =
    this.copy(parentAttributes = Some(parentAttributes))

  /**
    * Add an outer query (e.g., WITH query) to the context
    */
  def withOuterQuery(name: String, relation: LogicalPlan): AnalyzerContext =
    this.copy(outerQueries = outerQueries + (name -> relation))

///**
//  */
//object Analyzer extends LogSupport:
//
//  def analyze(sql: String, database: String, catalog: Catalog): LogicalPlan =
//    trace(s"analyze:\n${sql}")
//    analyze(FlowParser.parse(sql), database, catalog)
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
