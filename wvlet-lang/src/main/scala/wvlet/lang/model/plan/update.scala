package wvlet.lang.model.plan

import wvlet.lang.api.{LinePosition, Span}
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.expr.{Expression, Identifier, LeafExpression, NameExpr, QualifiedName}
import wvlet.lang.model.plan.{DDL, HasRefName, LeafPlan, LogicalPlan, Relation, UnaryRelation}
import wvlet.lang.model.RelationType

import scala.collection.immutable.ListMap

/**
  * A base trait for all update operations (e.g., add/delete the table contents).
  */
trait Update extends TopLevelStatement:
  override def relationType: RelationType = EmptyRelationType

trait Save extends Update with UnaryRelation:
  def targetName: String

sealed trait SaveToTable extends Save with HasRefName

case class SaveOption(key: Identifier, value: Expression, span: Span) extends LeafExpression

case class SaveTo(child: Relation, target: QualifiedName, saveOptions: List[SaveOption], span: Span)
    extends SaveToTable:
  override def targetName: String = target.fullName
  override def refName: NameExpr  = target

case class SaveToFile(child: Relation, path: String, saveOptions: List[SaveOption], span: Span)
    extends Save:
  override def targetName: String = path

case class AppendTo(child: Relation, target: QualifiedName, span: Span) extends SaveToTable:
  override def targetName: String = target.fullName
  override def refName: NameExpr  = target

case class AppendToFile(child: Relation, path: String, span: Span) extends Save:
  override def targetName: String = path

trait DeleteOps extends Update with UnaryRelation

case class Delete(child: Relation, targetTable: QualifiedName, span: Span) extends DeleteOps

case class DeleteFromFile(child: Relation, path: String, span: Span) extends DeleteOps

case class Truncate(targetTable: QualifiedName, nodeLocation: Option[LinePosition])
