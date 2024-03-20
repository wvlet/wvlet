package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.{NodeLocation, RelationType}
import com.treasuredata.flow.lang.model.expr.*

/*
 * SQL statements for changing the table schema or catalog
 */
sealed trait DDL extends LogicalPlan with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil

case class TypeAlias(
    alias: String,
    sourceTypeName: String,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class TypeDef(
    name: String,
    params: Seq[TypeParam],
    elems: Seq[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends DDL

case class TypeParam(name: String, value: String, nodeLocation: Option[NodeLocation]) extends Expression:
  override def toString: String          = s"${name}:${value}"
  override def children: Seq[Expression] = Seq.empty

// type elements (def or column definition)
sealed trait TypeElem extends Expression

case class TypeDefDef(name: String, tpe: Option[String], expr: Expression, nodeLocation: Option[NodeLocation])
    extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class TypeValDef(name: String, tpe: String, nodeLocation: Option[NodeLocation]) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class FunctionDef(
    name: String,
    args: Seq[FunctionArg],
    resultType: Option[String],
    bodyExpr: Expression,
    nodeLocation: Option[NodeLocation]
) extends DDL

case class FunctionArg(name: String, tpe: String, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class TableDef(
    name: String,
    params: Seq[TableDefParam],
    nodeLocation: Option[NodeLocation]
) extends DDL:
  def getType: Option[String] =
    params
      .find(_.name == "type")
      .map(_.paramValue)
      .collect {
        case l: Literal => l.stringValue
        case q: QName   => q.fullName
      }

case class TableDefParam(
    name: String,
    paramValue: Expression,
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq(paramValue)

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

  override def relationType: RelationType = query.relationType

case class InsertInto(
    table: QName,
    columnAliases: Option[Seq[Identifier]],
    query: Relation,
    nodeLocation: Option[NodeLocation]
) extends Update
    with UnaryRelation:
  override def child: Relation = query

  override def outputAttributes: Seq[Attribute] = Nil
  override def relationType: RelationType       = query.relationType

case class Delete(table: QName, where: Option[Expression], nodeLocation: Option[NodeLocation])
    extends Update
    with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil
