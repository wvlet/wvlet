package com.treasuredata.flow.lang.catalog

import com.treasuredata.flow.lang.catalog.SQLFunction.FunctionType
import com.treasuredata.flow.lang.model.DataType

trait SQLFunction:
  def name: String
  def functionType: SQLFunction.FunctionType
  def args: Seq[DataType]
  def returnType: DataType
  def properties: Map[String, Any]

object SQLFunction:
  enum FunctionType:
    case SCALAR,
      AGGREGATE,
      TABLE,
      PRAGMA,
      MACRO

case class BoundFunction(
    name: String,
    functionType: FunctionType,
    args: Seq[DataType],
    returnType: DataType,
    properties: Map[String, Any]
) extends SQLFunction:
  require(args.forall(_.isBound), s"Found unbound arguments: ${this}")
  require(returnType.isBound, s"return type: ${returnType} is not bound")

case class UnboundFunction(
    name: String,
    functionType: FunctionType,
    args: Seq[DataType],
    returnType: DataType,
    properties: Map[String, Any]
) extends SQLFunction:
  def bind(typeArgMap: Map[String, DataType]): BoundFunction = BoundFunction(
    name,
    functionType,
    args.map(_.bind(typeArgMap)),
    returnType.bind(typeArgMap),
    properties
  )
