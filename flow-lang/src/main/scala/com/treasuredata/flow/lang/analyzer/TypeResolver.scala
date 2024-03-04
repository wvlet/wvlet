package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.analyzer.RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.model.expr.{Attribute, AttributeIndex, AttributeList, ColumnType, Expression}
import com.treasuredata.flow.lang.model.plan.{
  Filter,
  FlowPlan,
  LogicalPlan,
  Project,
  Query,
  Relation,
  SchemaDef,
  TableRef,
  TableScan,
  TypeDef,
  TypeParam
}
import wvlet.log.LogSupport

import scala.util.control.NonFatal

object TypeResolver extends LogSupport:

  def defaultRules: List[RewriteRule] =
    resolveTableRef ::
      resolveRelation ::
      resolveProjectedColumns ::
      Nil

  def resolve(plan: LogicalPlan, context: AnalyzerContext): LogicalPlan =
    val resolvedPlan = defaultRules.foldLeft(plan) { (p, rule) =>
      try rule.transform(p, context)
      catch
        case NonFatal(e) =>
          debug(s"Failed to resolve with: ${rule.name}\n${p.pp}")
          throw e
    }
    resolvedPlan

  def resolveRelation(plan: LogicalPlan, context: AnalyzerContext): Relation =
    val resolvedPlan = resolve(plan, context)
    resolvedPlan match
      case r: Relation => r
      case _ =>
        throw StatusCode.NOT_A_RELATION.newException(s"Not a relation:\n${resolvedPlan.pp}")

  /**
    * Resolve TableRefs with concrete TableScans using the table schema in the catalog.
    */
  object resolveTableRef extends RewriteRule:
    def apply(context: AnalyzerContext): PlanRewriter = { case ref: TableRef =>
      context.findSchema(ref.name.fullName) match
        case Some(schema) =>
          TableScan(schema.typeName, schema, schema.columnTypes, ref.nodeLocation)
        case None =>
          ref
    }

  /**
    * Resolve expression in relation nodes
    */
  object resolveRelation extends RewriteRule:
    def apply(context: AnalyzerContext): PlanRewriter = {
      case q: Query =>
        q.copy(body = resolveRelation(q.body, context))
      case r: Relation => // Regular relation and Filter etc.
        r.transformUpExpressions { case x: Expression =>
          resolveExpression(x, r.inputAttributeList, context)
        }
    }

  /**
    * Resolve select items (projected attributes) in Project nodes
    */
  object resolveProjectedColumns extends RewriteRule:
    def apply(context: AnalyzerContext): PlanRewriter = { case p: Project =>
      val resolvedChild = resolveRelation(p.child, context)
      val resolvedColumns: Seq[Attribute] =
        resolveAttributes(p.selectItems, resolvedChild.outputAttributeList, context)
      Project(resolvedChild, resolvedColumns, p.nodeLocation)
    }

  /**
    * Resolve the given list of attribute types using known attributes from the child plan nodes as hints
    * @param attributes
    * @param knownAttributes
    * @param context
    * @return
    */
  private def resolveAttributes(
      attributes: Seq[Attribute],
      knownAttributes: AttributeList,
      context: AnalyzerContext
  ): Seq[Attribute] =
    attributes.map { a =>
      val resolvedExpr = resolveExpression(a, knownAttributes, context)
      a
    }

  /**
    * Resolve the given expression type using the input attributes from child plan nodes
    * @param expr
    * @param knownAttributes
    */
  private def resolveExpression(
      expr: Expression,
      knownAttributes: AttributeList,
      context: AnalyzerContext
  ): Expression =
    trace(s"resolve ${expr} using ${knownAttributes}")
    expr
