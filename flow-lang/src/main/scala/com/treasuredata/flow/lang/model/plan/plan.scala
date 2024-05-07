package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.compiler.SourceFile
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.expr.{Attribute, Expression}
import com.treasuredata.flow.lang.model.plan.LogicalPlan

sealed trait LanguageStatement extends LogicalPlan with LeafPlan:
  override def isEmpty: Boolean                 = true
  override def children: Seq[LogicalPlan]       = Nil
  override def outputAttributes: Seq[Attribute] = Nil
  override def inputAttributes: Seq[Attribute]  = Nil

// Top-level definition for each source file
case class PackageDef(
    name: Option[String],
    statements: Seq[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement:
  override def isEmpty: Boolean                 = statements.isEmpty
  override def children: Seq[LogicalPlan]       = statements
  override def outputAttributes: Seq[Attribute] = Nil

  override def inputAttributes: Seq[Attribute] = Nil

case class TestDef(
    testExprs: Seq[Expression],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

case class ImportDef(
    importRef: String,
    alias: Option[String],
    fromSource: Option[String],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

case class ModuleDef(
    name: String,
    elems: Seq[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

case class TypeAlias(
    alias: String,
    sourceTypeName: String,
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

case class TypeDef(
    name: String,
    elems: Seq[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

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
) extends LanguageStatement

case class FunctionArg(name: String, tpe: String, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq.empty
