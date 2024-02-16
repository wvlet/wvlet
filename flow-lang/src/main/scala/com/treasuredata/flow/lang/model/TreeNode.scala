package com.treasuredata.flow.lang.model

/**
  * A base class for LogicalPlan and Expression
  */
trait TreeNode[Elem <: TreeNode[Elem]]:
  def children: Seq[Elem]

  /**
    * @return
    *   the code location in the SQL text if available
    */
  def nodeLocation: Option[NodeLocation]
