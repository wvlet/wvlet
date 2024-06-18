package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.{NodeLocation, RelationType}
import com.treasuredata.flow.lang.model.expr.*

/*
 * SQL statements for changing the table schema or catalog
 */
sealed trait DDL extends LogicalPlan with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil

case class TableDef(
    name: Name,
    params: Seq[TableDefParam],
    nodeLocation: Option[NodeLocation]
) extends DDL:

  def getParam(paramName: Name): Option[Name] =
    params.find(_.name == paramName).map(_.paramValue)

  def getType: Option[Name] =
    params
      .find(_.name.fullName == "type")
      .map(_.paramValue)

case class TableDefParam(
    name: Name,
    paramValue: Name,
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Nil

case class CreateSchema(
    schema: Name,
    ifNotExists: Boolean,
    properties: Option[Seq[SchemaProperty]],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropDatabase(database: Name, ifExists: Boolean, cascade: Boolean, nodeLocation: Option[NodeLocation])
    extends DDL

case class RenameDatabase(database: Name, renameTo: Name, nodeLocation: Option[NodeLocation]) extends DDL

case class CreateTable(
    table: Name,
    ifNotExists: Boolean,
    tableElems: Seq[TableElement],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropTable(table: Name, ifExists: Boolean, nodeLocation: Option[NodeLocation]) extends DDL

case class RenameTable(table: Name, renameTo: Name, nodeLocation: Option[NodeLocation]) extends DDL

case class RenameColumn(table: Name, column: Name, renameTo: Name, nodeLocation: Option[NodeLocation]) extends DDL

case class DropColumn(table: Name, column: Name, nodeLocation: Option[NodeLocation]) extends DDL

case class AddColumn(table: Name, column: ColumnDef, nodeLocation: Option[NodeLocation]) extends DDL

case class CreateView(viewName: Name, replace: Boolean, query: Relation, nodeLocation: Option[NodeLocation]) extends DDL

case class DropView(viewName: Name, ifExists: Boolean, nodeLocation: Option[NodeLocation]) extends DDL

/**
  * A base trait for all update operations (e.g., add/delete the table contents).
  */
trait Update extends LogicalPlan

case class CreateTableAs(
    table: Name,
    ifNotEotExists: Boolean,
    columnAliases: Option[Seq[Identifier]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends DDL
    with Update
    with UnaryRelation:
  override def child: Relation = query

  override def relationType: RelationType = query.relationType

case class InsertInto(
    table: Name,
    columnAliases: Option[Seq[Name]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends Update
    with UnaryRelation:
  override def child: Relation = query

  override def outputAttributes: Seq[Attribute] = Nil
  override def relationType: RelationType       = query.relationType

case class Delete(table: Name, where: Option[Expression], nodeLocation: Option[NodeLocation])
    extends Update
    with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil
