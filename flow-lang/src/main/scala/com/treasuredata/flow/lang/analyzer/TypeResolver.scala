package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.analyzer.RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.analyzer.Type.{ExtensionType, NamedType, RecordType, UnresolvedType}
import com.treasuredata.flow.lang.model.expr.{AttributeIndex, AttributeList, ColumnType, Expression}
import com.treasuredata.flow.lang.model.plan.{
  Filter,
  FlowPlan,
  LogicalPlan,
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
          TableScan(schema.typeName, schema, schema.typeDefs, ref.nodeLocation)
        case None =>
          ref
    }

  object resolveRelation extends RewriteRule:
    def apply(context: AnalyzerContext): PlanRewriter = {
      case q: Query =>
        q.copy(body = resolveRelation(q.body, context))
      case r: Relation => // Regular relation and Filter etc.
        r.transformUpExpressions { case x: Expression =>
          resolveExpression(x, context, r.inputAttributeList)
        }
    }

  private def resolveExpression(
      expr: Expression,
      context: AnalyzerContext,
      inputAttributes: AttributeList
  ): Expression =
    trace(s"resolve ${expr} using ${inputAttributes}")
    expr
