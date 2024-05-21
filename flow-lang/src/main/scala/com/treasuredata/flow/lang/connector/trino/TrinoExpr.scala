package com.treasuredata.flow.lang.connector.trino

import com.treasuredata.flow.lang.connector.common.exprs.*

class TrinoString(using ctx: TrinoContext) extends IString:
  override def toInt: Expr[Int]         = e"cast(${self} as int)"
  override def toLong: Expr[Long]       = e"cast(${self} as bigint)"
  override def toFloat: Expr[Float]     = e"cast(${self} as real)"
  override def toDouble: Expr[Double]   = e"cast(${self} as double)"
  override def toBoolean: Expr[Boolean] = e"cast(${self} as boolean)"

  override def length: Expr[Int] = e"length(${self})"

  override def substring(start: Expr[Int]): Expr[String]                 = e"substring(${self}, ${start})"
  override def substring(start: Expr[Int], end: Expr[Int]): Expr[String] = e"substring(${self}, ${start}, ${end})"
  override def regexpContains(pattern: Expr[String]): Expr[Boolean]      = e"regexp_like(${self}, ${pattern})"
