package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*

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
    target: TableOrFileName,
    assignments: List[UpdateAssignment],
    cond: Option[Expression],
    span: Span
) extends Update
    with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class UpdateAssignment(target: NameExpr, expr: Expression, span: Span) extends Expression:
  override def children: Seq[Expression] = Seq(target, expr)

trait InsertOps extends Save:
  def columns: List[NameExpr]
  def query: Relation
  override def child: Relation = query

case class Insert(target: TableOrFileName, columns: List[NameExpr], query: Relation, span: Span)
    extends InsertOps

case class Upsert(target: TableOrFileName, columns: List[NameExpr], query: Relation, span: Span)
    extends InsertOps

case class Merge(
    target: TableOrFileName,
    alias: Option[NameExpr],
    using: Relation,
    on: Expression,
    whenMatched: Option[List[UpdateAssignment]],
    whenNotMatchedInsert: Option[Values],
    span: Span
) extends Save:
  override def relationType: RelationType = EmptyRelationType
  override def child: Relation            = using

case class PrepareStatement(name: NameExpr, statement: LogicalPlan, span: Span)
    extends DDL
    with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class ExecuteStatement(name: NameExpr, parameters: List[Expression], span: Span)
    extends DDL
    with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class DeallocateStatement(name: NameExpr, span: Span) extends DDL with LeafPlan:
  override def relationType: RelationType = EmptyRelationType
