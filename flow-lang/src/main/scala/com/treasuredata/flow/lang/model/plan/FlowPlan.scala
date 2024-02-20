package com.treasuredata.flow.lang.model.plan

case class FlowPlan(plans: Seq[LogicalPlan]):
  override def toString: String =
    plans.filter(_ != null).map(_.pp).mkString("\n")
