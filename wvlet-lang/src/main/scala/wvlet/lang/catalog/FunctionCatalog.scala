package wvlet.lang.catalog

import wvlet.lang.catalog.SQLFunction.FunctionType
import wvlet.lang.model.DataType

object SQLFunction:
  enum FunctionType:
    case SCALAR,
      AGGREGATE,
      TABLE,
      PRAGMA,
      MACRO,
      WINDOW,
      UNKNOWN

case class SQLFunction(
    name: String,
    functionType: FunctionType,
    args: Seq[DataType],
    returnType: DataType,
    properties: Map[String, Any] = Map.empty
)
