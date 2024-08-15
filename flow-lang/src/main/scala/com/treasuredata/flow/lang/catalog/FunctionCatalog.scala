package com.treasuredata.flow.lang.catalog

import com.treasuredata.flow.lang.catalog.SQLFunction.FunctionType
import com.treasuredata.flow.lang.model.DataType

object SQLFunction:
  enum FunctionType:
    case SCALAR,
      AGGREGATE,
      TABLE,
      PRAGMA,
      MACRO,
      UNKNOWN

case class SQLFunction(
    name: String,
    functionType: FunctionType,
    args: Seq[DataType],
    returnType: DataType,
    properties: Map[String, Any] = Map.empty
)
