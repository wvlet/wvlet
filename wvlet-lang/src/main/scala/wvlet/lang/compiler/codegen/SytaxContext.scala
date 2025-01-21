package wvlet.lang.compiler.codegen

sealed trait SyntaxContext:
  import SyntaxContext.*
  def inFromClause: Boolean = this == InFromClause
  def isNested: Boolean     = this != InStatement

object SyntaxContext:
  case object InStatement  extends SyntaxContext
  case object InSubQuery   extends SyntaxContext
  case object InFromClause extends SyntaxContext
