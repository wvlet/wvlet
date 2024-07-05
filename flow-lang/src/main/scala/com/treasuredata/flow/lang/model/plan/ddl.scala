package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.DataType.EmptyRelationType
import com.treasuredata.flow.lang.model.{NodeLocation, RelationType}
import com.treasuredata.flow.lang.model.expr.*

/*
 * SQL statements for changing the table schema or catalog
 */
sealed trait DDL extends LogicalPlan with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class TableDef(name: NameExpr, params: Seq[TableDefParam], nodeLocation: Option[NodeLocation])
    extends DDL:

  def getParam(paramName: NameExpr): Option[NameExpr] = params
    .find(_.name == paramName)
    .map(_.paramValue)

  def getType: Option[NameExpr] = params.find(_.name.fullName == "type").map(_.paramValue)

case class TableDefParam(name: NameExpr, paramValue: NameExpr, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Nil

case class CreateSchema(
    schema: NameExpr,
    ifNotExists: Boolean,
    properties: Option[Seq[SchemaProperty]],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropDatabase(
    database: NameExpr,
    ifExists: Boolean,
    cascade: Boolean,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class RenameDatabase(
    database: NameExpr,
    renameTo: NameExpr,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class CreateTable(
    table: NameExpr,
    ifNotExists: Boolean,
    tableElems: Seq[TableElement],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropTable(table: NameExpr, ifExists: Boolean, nodeLocation: Option[NodeLocation])
    extends DDL

case class RenameTable(table: NameExpr, renameTo: NameExpr, nodeLocation: Option[NodeLocation])
    extends DDL

case class RenameColumn(
    table: NameExpr,
    column: NameExpr,
    renameTo: NameExpr,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropColumn(table: NameExpr, column: NameExpr, nodeLocation: Option[NodeLocation])
    extends DDL

case class AddColumn(table: NameExpr, column: ColumnDef, nodeLocation: Option[NodeLocation])
    extends DDL

case class CreateView(
    viewName: NameExpr,
    replace: Boolean,
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class DropView(viewName: NameExpr, ifExists: Boolean, nodeLocation: Option[NodeLocation])
    extends DDL

/**
  * A base trait for all update operations (e.g., add/delete the table contents).
  */
trait Update extends LogicalPlan

case class CreateTableAs(
    table: NameExpr,
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
    table: NameExpr,
    columnAliases: Option[Seq[NameExpr]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends Update
    with UnaryRelation:
  override def child: Relation            = query
  override def relationType: RelationType = query.relationType

case class Delete(table: NameExpr, where: Option[Expression], nodeLocation: Option[NodeLocation])
    extends Update
    with LeafPlan:
  override def relationType: RelationType = EmptyRelationType
