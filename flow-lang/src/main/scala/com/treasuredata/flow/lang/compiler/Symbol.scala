package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.expr.{Name, NoName}

case class Symbol(name: Name)

object Symbol:
  val NoSymbol = Symbol(NoName)
