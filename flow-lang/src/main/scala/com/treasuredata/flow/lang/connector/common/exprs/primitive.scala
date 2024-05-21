package com.treasuredata.flow.lang.connector.common.exprs

import com.treasuredata.flow.lang.connector.DBContext

trait IBoolean(using ctx: DBContext):
  export ctx.self

  // !x
  def unary_! : Expr[Boolean]             = e"not ${self}"
  def ==(x: Expr[Boolean]): Expr[Boolean] = e"${self} = ${x}"
  def !=(x: Expr[Boolean]): Expr[Boolean] = e"${self} != ${x}"

  def and(x: Expr[Boolean]): Expr[Boolean] = e"${self} and ${x}"
  def or(x: Expr[Boolean]): Expr[Boolean]  = e"${self} or ${x}"

  def &&(x: Expr[Boolean]): Expr[Boolean] = and(x)
  def ||(x: Expr[Boolean]): Expr[Boolean] = or(x)

trait IInt(using ctx: DBContext):
  export ctx.self

  def toBoolean: Expr[Boolean]
  def toStr: Expr[String]
  def toLong: Expr[Long]
  def toFloat: Expr[Float]
  def toDouble: Expr[Double]

  def +[X: Numeric](x: Expr[X]): Expr[X] = e"${self} + ${x}"
  def -[X: Numeric](x: Expr[X]): Expr[X] = e"${self} - ${x}"
  def *[X: Numeric](x: Expr[X]): Expr[X] = e"${self} * ${x}"
  def /[X: Numeric](x: Expr[X]): Expr[X] = e"${self} / ${x}"
  def %[X: Numeric](x: Expr[X]): Expr[X] = e"${self} % ${x}"

//class Long:
//  def +(x: Int): Long
//  def +(x: Long): Long
//  def +(x: Float): Float
//  def +(x: Double): Double
//
//  def -(x: Int): Long
//  def -(x: Long): Long
//  def -(x: Float): Float
//  def -(x: Double): Double
//
//  def *(x: Int): Long
//  def *(x: Long): Long
//  def *(x: Float): Float
//  def *(x: Double): Double
//
//  def /(x: Int): Long
//  def /(x: Long): Long
//  def /(x: Float): Float
//  def /(x: Double): Double
//
//  def %(x: Int): Long
//  def %(x: Long): Long
//  def %(x: Float): Float
//  def %(x: Double): Double
//end
//
//class Float:
//  def +(x: Int): Float
//  def +(x: Long): Float
//  def +(x: Float): Float
//  def +(x: Double): Double
//
//  def -(x: Int): Float
//  def -(x: Long): Float
//  def -(x: Float): Float
//  def -(x: Double): Double
//
//  def *(x: Int): Float
//  def *(x: Long): Float
//  def *(x: Float): Float
//  def *(x: Double): Double
//
//  def /(x: Int): Float
//  def /(x: Long): Float
//  def /(x: Float): Float
//  def /(x: Double): Double
//
//  def %(x: Int): Float
//  def %(x: Long): Float
//  def %(x: Float): Float
//  def %(x: Double): Double
//end
//
//
//}
