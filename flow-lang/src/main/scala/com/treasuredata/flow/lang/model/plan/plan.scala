package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.compiler.SourceFile
import com.treasuredata.flow.lang.model.DataType.TypeParameter
import com.treasuredata.flow.lang.model.{DataType, NodeLocation, RelationType}
import com.treasuredata.flow.lang.model.expr.{Attribute, Expression, Name, StringLiteral}
import com.treasuredata.flow.lang.model.plan.LogicalPlan

sealed trait LanguageStatement extends LogicalPlan with LeafPlan:
  override def isEmpty: Boolean                 = true
  override def children: Seq[LogicalPlan]       = Nil
  override def outputAttributes: Seq[Attribute] = Nil
  override def inputAttributes: Seq[Attribute]  = Nil

trait HasSourceFile:
  def sourceFile: SourceFile

// Top-level definition for each source file
case class PackageDef(
    name: Option[Expression],
    statements: List[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement
    with HasSourceFile:
  override def isEmpty: Boolean                 = statements.isEmpty
  override def children: Seq[LogicalPlan]       = statements
  override def outputAttributes: Seq[Attribute] = Nil

  override def inputAttributes: Seq[Attribute] = Nil

case class Import(
    importRef: Name,
    alias: Option[Name],
    fromSource: Option[StringLiteral],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

//case class ModuleDef(
//    name: Name,
//    elems: Seq[TypeElem],
//    nodeLocation: Option[NodeLocation]
//) extends LanguageStatement

case class TypeAlias(alias: Name, sourceTypeName: Name, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

case class TypeDef(
    name: Name,
    params: List[TypeParameter],
    scopes: List[DefScope],
    parent: Option[Name],
    elems: List[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

// type elements (def or column definition)
sealed trait TypeElem extends Expression

case class TopLevelFunctionDef(functionDef: FunctionDef, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

// def ... { ... } in the type definition
case class FunctionDef(
    name: Name,
    args: List[DefArg],
    scopes: List[DefScope],
    retType: Option[DataType],
    expr: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class DefArg(
    name: Name,
    override val dataType: DataType,
    defaultValue: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class TypeValDef(
    name: Name,
    tpe: Name,
    body: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class DefScope(name: Option[Name], tpe: Name, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq.empty
