package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.model.Expression.*
import wvlet.log.LogSupport

trait LeafPlan extends LogicalPlan:
  override def children: Seq[LogicalPlan] = Nil

trait UnaryPlan extends LogicalPlan:
  def child: LogicalPlan

  override def children: Seq[LogicalPlan] = child :: Nil

trait BinaryPlan extends LogicalPlan:
  def left: LogicalPlan
  def right: LogicalPlan
  override def children: Seq[LogicalPlan] = Seq(left, right)

sealed trait Relation extends LogicalPlan

// A relation that takes a single input relation
sealed trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation = child

  override def child: Relation

case class ParenthesizedRelation(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation

case class AliasedRelation(
    child: Relation,
    alias: Identifier,
    columnNames: Option[Seq[String]],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:

  override def toString: String =
    columnNames match
      case Some(columnNames) =>
        s"AliasedRelation[${alias}](Select[${columnNames.mkString(", ")}](${child}))"
      case None =>
        s"AliasedRelation[${alias}](${child})"

case class Values(rows: Seq[Expression], nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String = s"Values(${rows.mkString(", ")})"

case class TableRef(name: QName, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String = s"TableRef(${name})"

case class RawSQL(sql: String, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan {}

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String = s"Distinct(${child})"

case class Sort(child: Relation, orderBy: Seq[SortItem], nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String = s"Sort[${orderBy.mkString(", ")}](${child})"

case class Limit(child: Relation, limit: LongLiteral, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String = s"Limit[${limit.value}](${child})"

case class Filter(child: Relation, filterExpr: Expression, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String = s"Filter[${filterExpr}](${child})"

case class EmptyRelation(nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends UnaryRelation:
  def selectItems: Seq[Attribute]

case class Project(child: Relation, selectItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:

  override def toString: String = s"Project[${selectItems.mkString(", ")}](${child})"

case class Aggregate(
    child: Relation,
    selectItems: List[Attribute],
    groupingKeys: List[GroupingKey],
    having: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection:

  override def toString =
    s"Aggregate[${groupingKeys.mkString(",")}](Select[${selectItems.mkString(", ")}](${child}))"

case class Query(body: Relation, nodeLocation: Option[NodeLocation]) extends Relation:

  override def children: Seq[LogicalPlan] =
    val b = Seq.newBuilder[LogicalPlan]
    b += body
    b.result()

  override def toString: String = s"Query(body:${body})"

// Joins
case class Join(
    joinType: JoinType,
    left: Relation,
    right: Relation,
    cond: JoinCriteria,
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LogSupport:
  override def modelName: String = joinType.toString

  override def children: Seq[LogicalPlan] = Seq(left, right)

  override def toString: String          = s"${joinType}[${cond}](left:${left}, right:${right})"
  def withCond(cond: JoinCriteria): Join = this.copy(cond = cond)

sealed abstract class JoinType(val symbol: String)

// Exact match (= equi join)
case object InnerJoin extends JoinType("J")

// Joins for preserving left table entries
case object LeftOuterJoin extends JoinType("LJ")

// Joins for preserving right table entries
case object RightOuterJoin extends JoinType("RJ")

// Joins for preserving both table entries
case object FullOuterJoin extends JoinType("FJ")

// Cartesian product of two tables
case object CrossJoin extends JoinType("CJ")

// From clause contains only table names, and
// Where clause specifies join criteria
case object ImplicitJoin extends JoinType("J")

sealed trait SetOperation extends Relation with LogSupport:
  override def children: Seq[Relation]

case class Intersect(
    relations: Seq[Relation],
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def children: Seq[Relation] = relations

  override def toString =
    s"Intersect(${relations.mkString(", ")})"

case class Except(left: Relation, right: Relation, nodeLocation: Option[NodeLocation]) extends SetOperation:
  override def children: Seq[Relation] = Seq(left, right)

  override def toString =
    s"Except(${left}, ${right})"

case class Union(
    relations: Seq[Relation],
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def children: Seq[Relation] = relations

  override def toString =
    s"Union(${relations.mkString(",")})"

case class Unnest(columns: Seq[Expression], withOrdinality: Boolean, nodeLocation: Option[NodeLocation])
    extends Relation:
  override def children: Seq[LogicalPlan] = Seq.empty

  override def toString =
    s"Unnest(withOrdinality:${withOrdinality}, ${columns.mkString(",")})"

case class Lateral(query: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def child: Relation = query

case class LateralView(
    child: Relation,
    exprs: Seq[Expression],
    tableAlias: Identifier,
    columnAliases: Seq[Identifier],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation

/*
 * SQL statements for changing the table schema or catalog
 */
sealed trait DDL extends LogicalPlan with LeafPlan

case class CreateSchema(
    schema: QName,
    ifNotExists: Boolean,
    properties: Option[Seq[SchemaProperty]],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropDatabase(database: QName, ifExists: Boolean, cascade: Boolean, nodeLocation: Option[NodeLocation])
    extends DDL

case class RenameDatabase(database: QName, renameTo: Identifier, nodeLocation: Option[NodeLocation]) extends DDL

case class CreateTable(
    table: QName,
    ifNotExists: Boolean,
    tableElems: Seq[TableElement],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropTable(table: QName, ifExists: Boolean, nodeLocation: Option[NodeLocation]) extends DDL

case class RenameTable(table: QName, renameTo: QName, nodeLocation: Option[NodeLocation]) extends DDL

case class RenameColumn(table: QName, column: Identifier, renameTo: Identifier, nodeLocation: Option[NodeLocation])
    extends DDL

case class DropColumn(table: QName, column: Identifier, nodeLocation: Option[NodeLocation]) extends DDL

case class AddColumn(table: QName, column: ColumnDef, nodeLocation: Option[NodeLocation]) extends DDL

case class CreateView(viewName: QName, replace: Boolean, query: Relation, nodeLocation: Option[NodeLocation])
    extends DDL

case class DropView(viewName: QName, ifExists: Boolean, nodeLocation: Option[NodeLocation]) extends DDL

/**
  * A base trait for all update operations (e.g., add/delete the table contents).
  */
trait Update extends LogicalPlan

case class CreateTableAs(
    table: QName,
    ifNotEotExists: Boolean,
    columnAliases: Option[Seq[Identifier]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends DDL
    with Update
    with UnaryRelation:
  override def child: Relation = query

case class InsertInto(
    table: QName,
    columnAliases: Option[Seq[Identifier]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends Update
    with UnaryRelation:
  override def child: Relation = query

case class Delete(table: QName, where: Option[Expression], nodeLocation: Option[NodeLocation])
    extends Update
    with LeafPlan {}
