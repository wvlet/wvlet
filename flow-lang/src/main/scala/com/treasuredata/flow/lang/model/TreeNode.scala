package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.compiler.{
  Context,
  CompilationUnit,
  SourceFile,
  SourceLocation,
  Symbol
}

/**
  * A base class for LogicalPlan and Expression
  */
trait TreeNode:

  private var _symbol: Symbol = Symbol.NoSymbol

  def symbol: Symbol            = _symbol
  def symbol_=(s: Symbol): Unit = _symbol = s

  /**
    * @return
    *   the code location in the SQL text if available
    */
  def nodeLocation: Option[NodeLocation]
  def sourceLocation(using cu: CompilationUnit): SourceLocation = SourceLocation(cu, nodeLocation)
  def locationString(using ctx: Context): String =
    sourceLocation(using ctx.compilationUnit).locationString
