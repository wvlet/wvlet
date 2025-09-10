package wvlet.lang.model.plan

import wvlet.lang.api.{LinePosition, Span}
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

trait Save extends Update with UnaryRelation

case class SaveOption(key: Identifier, value: Expression, span: Span) extends LeafExpression

/**
  * Partition write strategies for ETL operations
  */
enum PartitionWriteMode:
  case HIVE_CLUSTER_BY
  case HIVE_DISTRIBUTE_BY
  case HIVE_SORT_BY

/**
  * Generic partition write options for ETL operations
  */
case class PartitionWriteOption(
    mode: PartitionWriteMode,
    expressions: List[Expression] = Nil,
    sortItems: List[SortItem] = Nil
)

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
