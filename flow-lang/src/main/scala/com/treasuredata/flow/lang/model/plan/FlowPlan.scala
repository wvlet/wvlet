package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.CompileUnit

case class FlowPlan(plans: Seq[LogicalPlan], compileUnit: CompileUnit = CompileUnit.empty):
  override def toString: String =
    plans.filter(_ != null).map(_.pp).mkString("\n")

  def withCompileUnit(compileUnit: CompileUnit): FlowPlan =
    copy(compileUnit = compileUnit)
