package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
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
) extends DDL

case class ExplainPlan(child: LogicalPlan, span: Span) extends TopLevelStatement with UnaryPlan:
  override def relationType: RelationType = EmptyRelationType

//enum DescribeTarget:
//  case DATABASE
//  case CATALOG
//  case SCHEMA
//  case TABLE
//
//case class DescribeStmt(target: DescribeTarget, name: QualifiedName, span: Span)
//    extends TopLevelStatement
//    with LeafPlan:
//  override def relationType: RelationType = EmptyRelationType

case class UpdateRows(
    target: QualifiedName,
    assignments: List[UpdateAssignment],
    cond: Option[Expression],
    span: Span
) extends Update
    with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class UpdateAssignment(target: NameExpr, expr: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(target, expr)

trait InsertOps extends Save:
  def target: QualifiedName
  override def targetName: String = target.fullName
  def columns: List[NameExpr]
  def query: Relation
  override def child: Relation = query

case class Insert(target: QualifiedName, columns: List[NameExpr], query: Relation, span: Span)
    extends InsertOps

case class Upsert(target: QualifiedName, columns: List[NameExpr], query: Relation, span: Span)
    extends InsertOps

case class Merge(
    target: QualifiedName,
    alias: Option[NameExpr],
    using: Relation,
    on: Expression,
    whenMatched: Option[List[UpdateAssignment]],
    whenNotMatchedInsert: Option[Values],
    span: Span
) extends Save:
  override def targetName: String         = target.fullName
  override def relationType: RelationType = EmptyRelationType
  override def child: Relation            = using

case class UseSchema(schema: QualifiedName, span: Span) extends TopLevelStatement with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class WithQuery(
    isRecursive: Boolean,
    withStatements: List[AliasedRelation],
    child: Relation,
    span: Span
) extends Relation:
  override def children: Seq[LogicalPlan] = withStatements :+ child
  override def relationType: RelationType = child.relationType
