package com.treasuredata.flow.lang.connector.common.exprs

/**
  * An abstraction of expressions for mapping AST to SQL expressions
  * @param str
  * @tparam A
  */
class Expr[A](str: String):
  override def toString: String = toText
  def toText: String            = str

extension (sc: StringContext)
  def e[A](args: Any*): Expr[A] =
    Expr[A](args.mkString)
