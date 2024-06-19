package com.treasuredata.flow.lang.catalog

import com.treasuredata.flow.lang.model.DataType

/**
  * Manage the list of unbounded functions, whose types are not resolved yet.
  */
trait FunctionCatalog:
  def listFunctions: Seq[SQLFunction]

trait SQLFunction:
  def name: String
  def args: Seq[DataType]
  def returnType: DataType

trait SQLFunctionType:
  def dataType: DataType

case class BoundFunction(name: String, args: Seq[DataType], returnType: DataType)
    extends SQLFunction:
  require(args.forall(_.isBound), s"Found unbound arguments: ${this}")
  require(returnType.isBound, s"return type: ${returnType} is not bound")

case class UnboundFunction(name: String, args: Seq[DataType], returnType: DataType)
    extends SQLFunction:
  def bind(typeArgMap: Map[String, DataType]): BoundFunction = BoundFunction(
    name,
    args.map(_.bind(typeArgMap)),
    returnType.bind(typeArgMap)
  )

//object UnboundFunction {
//  def parse(name: String, argTypeStr: String, returnTypeStr: String): SQLFunction = {
//    val argTypes = DataTypeParser.parseTypeList(argTypeStr)
//    val retType  = DataTypeParser.parse(returnTypeStr)
//    UnboundFunction(name, argTypes, retType)
//  }
//}
