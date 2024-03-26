package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.compiler.CompilationUnit

case class FlowPlan(logicalPlans: Seq[LogicalPlan], compileUnit: CompilationUnit = CompilationUnit.empty):
  override def toString: String =
    logicalPlans.filter(_ != null).map(_.pp).mkString("\n")

  def withCompileUnit(compileUnit: CompilationUnit): FlowPlan =
    copy(compileUnit = compileUnit)
