package com.treasuredata.flow.lang.connector.common.exprs

import com.treasuredata.flow.lang.connector.DBContext

trait IString(using ctx: DBContext):
  export ctx.self

  def toInt: Expr[Int]
  def toLong: Expr[Long]
  def toFloat: Expr[Float]
  def toDouble: Expr[Double]
  def toBoolean: Expr[Boolean]

  def `=`(s: Expr[String]): Expr[Boolean] = e"${self} = ${s}"
  def !=(s: Expr[String]): Expr[Boolean]  = e"${self} != ${s}"
  def <(s: Expr[String]): Expr[Boolean]   = e"${self} < ${s}"
  def <=(s: Expr[String]): Expr[Boolean]  = e"${self} <= ${s}"
  def >(s: Expr[String]): Expr[Boolean]   = e"${self} > ${s}"
  def >=(s: Expr[String]): Expr[Boolean]  = e"${self} >= ${s}"

  def length: Expr[Int]
  def substring(start: Expr[Int]): Expr[String]
  def substring(start: Expr[Int], end: Expr[Int]): Expr[String]
  def regexpContains(pattern: Expr[String]): Expr[Boolean]
