package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.model.expr.{Expression, Identifier, LeafExpression, NameExpr, QualifiedName}

enum AlterType:
  case DEFAULT
  case SYSTEM
  case SESSION

case class AlterVariable(
    tpe: AlterType,
    isReset: Boolean = false,
    identifier: Identifier,
    value: Option[Expression] = None,
    span: Span
) extends TopLevelStatement
    with LeafPlan

case class ExplainPlan(plan: LogicalPlan, span: Span) extends TopLevelStatement with UnaryPlan

enum DescribeTarget:
  case DATABASE
  case CATALOG
  case SCHEMA
  case TABLE
  case STATEMENT

case class DescribeStmt(target: DescribeTarget, name: NameExpr, span: Span)
    extends TopLevelStatement
    with LeafPlan

case class Update(
    target: NameExpr,
    assignments: List[UpdateAssignment],
    cond: Option[Expression],
    span: Span
) extends TopLevelStatement

case class UpdateAssignment(target: NameExpr, expr: Expression, span: Span) extends Expression

case class Insert(target: QualifiedName, columns: List[NameExpr], query: Relation, span: Span)
    extends UnaryRelation
    with TopLevelStatement

case class Upsert(target: QualifiedName, columns: List[NameExpr], query: Relation, span: Span)
    extends UnaryRelation
    with TopLevelStatement

case class Merge(
    target: QualifiedName,
    alias: Option[NameExpr],
    using: QualifiedName,
    on: Expression,
    whenMatched: Option[List[UpdateAssignment]],
    whenNotMatchedInsert: Option[Values],
    span: Span
)
