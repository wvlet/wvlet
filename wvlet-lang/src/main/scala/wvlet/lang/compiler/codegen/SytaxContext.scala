package wvlet.lang.compiler.codegen

sealed trait SyntaxContext:
  import SyntaxContext.*
  def inFromClause: Boolean = this == InFromClause
  def isNested: Boolean     = this == InSubQuery
  def inJoinClause: Boolean = this == InJoinClause

object SyntaxContext:
  case object InStatement  extends SyntaxContext
  case object InSubQuery   extends SyntaxContext
  case object InFromClause extends SyntaxContext
  case object InJoinClause extends SyntaxContext
