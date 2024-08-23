package wvlet.lang.model

import wvlet.lang.compiler.Context
import wvlet.lang.compiler.SourceLocation

case class NodeLocation(
    line: Int,
    // column position in the line (1-origin)
    column: Int
):
  override def toString: String = s"$line:$column"
  def toSourceLocation(using ctx: Context): SourceLocation = ctx
    .compilationUnit
    .toSourceLocation(Some(this))
