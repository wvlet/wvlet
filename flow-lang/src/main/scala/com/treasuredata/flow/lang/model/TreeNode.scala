package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile, SourceLocation}

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
  def sourceLocation(using cu: CompilationUnit): SourceLocation = SourceLocation(cu, nodeLocation)
  def locationString(using cu: CompilationUnit): String         = sourceLocation.locationString
