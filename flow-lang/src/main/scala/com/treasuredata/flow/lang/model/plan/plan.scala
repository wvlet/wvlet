package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.compiler.{SourceFile, TermName}
import com.treasuredata.flow.lang.model.DataType.{EmptyRelationType, TypeParameter}
import com.treasuredata.flow.lang.model.{DataType, NodeLocation, RelationType}
import com.treasuredata.flow.lang.model.expr.{
  Attribute,
  Expression,
  NameExpr,
  QualifiedName,
  StringLiteral
}
import com.treasuredata.flow.lang.model.plan.LogicalPlan

sealed trait LanguageStatement extends LogicalPlan with LeafPlan:
  override def isEmpty: Boolean           = true
  override def children: Seq[LogicalPlan] = Nil

  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

trait HasSourceFile:
  def sourceFile: SourceFile

// Top-level definition for each source file
case class PackageDef(
    name: QualifiedName,
    statements: List[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement
    with HasSourceFile:
  override def isEmpty: Boolean           = statements.isEmpty
  override def children: Seq[LogicalPlan] = statements

case class Import(
    importRef: NameExpr,
    alias: Option[NameExpr],
    fromSource: Option[StringLiteral],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

//case class MpoduleDef(
//    name: Name,
//    elems: Seq[TypeElem],
//    nodeLocation: Option[NodeLocation]
//) extends LanguageStatement

case class TypeAlias(alias: NameExpr, sourceTypeName: NameExpr, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

case class TypeDef(
    name: NameExpr,
    params: List[TypeParameter],
    scopes: List[DefContext],
    parent: Option[NameExpr],
    elems: List[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

// type elements (def or column definition)
sealed trait TypeElem extends Expression

case class TopLevelFunctionDef(functionDef: FunctionDef, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

// def ... { ... } in the type definition
case class FunctionDef(
    name: NameExpr,
    args: List[DefArg],
    scopes: List[DefContext],
    retType: Option[DataType],
    expr: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class DefArg(
    name: TermName,
    override val dataType: DataType,
    defaultValue: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class TypeValDef(
    name: NameExpr,
    tpe: NameExpr,
    body: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

/**
  * Definition scope (e.g., in xxx)
  * @param name
  * @param tpe
  * @param nodeLocation
  */
case class DefContext(name: Option[NameExpr], tpe: NameExpr, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq.empty
