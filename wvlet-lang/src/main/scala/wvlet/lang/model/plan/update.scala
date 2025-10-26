package wvlet.lang.model.plan

import wvlet.lang.api.LinePosition
import wvlet.lang.api.Span
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

import scala.collection.immutable.ListMap

/**
  * A base trait for all update operations (e.g., add/delete the table contents).
  */
trait Update extends TopLevelStatement with HasTableOrFileName:
  override def relationType: RelationType = EmptyRelationType

object Update:
  /**
    * Extract partition write options from a relation if it's a PartitioningHint, and return the
    * unwrapped child relation and the options.
    */
  def extractPartitionOptions(relation: Relation): (Relation, List[PartitionWriteOption]) =
    relation match
      case hint: PartitioningHint =>
        (hint.child, hint.partitionWriteOptions)
      case other =>
        (other, Nil)

trait Save extends Update with UnaryRelation

case class SaveOption(key: Identifier, value: Expression, span: Span) extends LeafExpression

case class SaveTo(
    child: Relation,
    target: TableOrFileName,
    saveOptions: List[SaveOption],
    span: Span
) extends Save

case class AppendTo(
    child: Relation,
    target: TableOrFileName,
    columns: List[NameExpr] = Nil,
    span: Span
) extends Save

/**
  * Delete qualifying rows from the target table
  * @param child
  * @param target
  * @param span
  */
case class Delete(child: Relation, target: TableOrFileName, span: Span) extends Save

case class Truncate(target: TableOrFileName, span: Span) extends Update with LeafPlan

// SQL equivalent operators

enum CreateMode:
  case NoOverwrite
  case IfNotExists
  case Replace

case class CreateTableAs(
    target: TableOrFileName,
    createMode: CreateMode,
    child: Relation,
    properties: List[(NameExpr, Expression)] = Nil,
    partitionWriteOptions: List[PartitionWriteOption] = Nil,
    span: Span
) extends Save:
  override def relationType: RelationType = EmptyRelationType

case class InsertInto(
    target: TableOrFileName,
    columns: List[NameExpr],
    child: Relation,
    partitionWriteOptions: List[PartitionWriteOption] = Nil,
    span: Span
) extends Save:
  override def relationType: RelationType = EmptyRelationType

case class InsertOverwrite(
    target: TableOrFileName,
    child: Relation,
    partitionWriteOptions: List[PartitionWriteOption] = Nil,
    span: Span
) extends Save
