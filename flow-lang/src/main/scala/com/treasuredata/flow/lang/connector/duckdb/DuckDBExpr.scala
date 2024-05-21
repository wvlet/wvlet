package com.treasuredata.flow.lang.connector.duckdb

import com.treasuredata.flow.lang.connector.common.exprs.*

class DuckDBString(using ctx: DuckDBContext) extends IString:
  override def toInt: Expr[Int]         = e"${self}::integer"
  override def toLong: Expr[Long]       = e"${self}::long"
  override def toFloat: Expr[Float]     = e"${self}::float"
  override def toDouble: Expr[Double]   = e"${self}::double"
  override def toBoolean: Expr[Boolean] = e"${self}::boolean"

  override def length: Expr[Int] = e"strlen(${self})"

  override def substring(start: Expr[Int]): Expr[String] = e"substring(${self}, ${start}, strlen(${self}))"
  override def substring(start: Expr[Int], end: Expr[Int]): Expr[String] = e"substring(${self}, ${start}, ${end})"
  override def regexpContains(pattern: Expr[String]): Expr[Boolean]      = e"regexp_matches(${self}, ${pattern})"
end DuckDBString

class DuckDBBoolean(using ctx: DuckDBContext) extends IBoolean:
  override def unary_! : Expr[Boolean]             = e"not ${self}"
  override def ==(x: Expr[Boolean]): Expr[Boolean] = e"${self} == ${x}"
  override def !=(x: Expr[Boolean]): Expr[Boolean] = e"${self} != ${x}"

  override def and(x: Expr[Boolean]): Expr[Boolean] = e"${self} and ${x}"
  override def or(x: Expr[Boolean]): Expr[Boolean]  = e"${self} or ${x}"
  override def &&(x: Expr[Boolean]): Expr[Boolean]  = and(x)
  override def ||(x: Expr[Boolean]): Expr[Boolean]  = or(x)
end DuckDBBoolean

class DuckDBInt(using ctx: DuckDBContext) extends IInt:
  override def toBoolean: Expr[Boolean] = e"${self}::boolean"
  override def toLong: Expr[Long]       = e"${self}::long"
  override def toFloat: Expr[Float]     = e"${self}::float"
  override def toDouble: Expr[Double]   = e"${self}::double"
  override def toStr: Expr[String]      = e"${self}::string"
end DuckDBInt
