package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.compiler.SourceFile
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.expr.{Attribute, Expression}
import com.treasuredata.flow.lang.model.plan.LogicalPlan

// Top-level definition for each source file
case class PackageDef(
    statements: Seq[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    nodeLocation: Option[NodeLocation]
) extends LogicalPlan:
  override def isEmpty: Boolean                 = statements.isEmpty
  override def children: Seq[LogicalPlan]       = statements
  override def outputAttributes: Seq[Attribute] = Nil

  override def inputAttributes: Seq[Attribute] = Nil

case class TestDef(
    testExprs: Seq[Expression],
    nodeLocation: Option[NodeLocation]
) extends LogicalPlan:
  override def isEmpty: Boolean                 = true
  override def children: Seq[LogicalPlan]       = Nil
  override def outputAttributes: Seq[Attribute] = Nil
  override def inputAttributes: Seq[Attribute]  = Nil
