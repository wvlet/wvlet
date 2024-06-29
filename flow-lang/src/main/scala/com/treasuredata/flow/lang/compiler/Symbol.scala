package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.expr.Name
import com.treasuredata.flow.lang.model.expr.Name.NoName

case class Symbol(name: Name):
  private var _dataType: DataType = DataType.UnknownType
  def dataType: DataType          = _dataType

object Symbol:
  val NoSymbol = Symbol(NoName)
