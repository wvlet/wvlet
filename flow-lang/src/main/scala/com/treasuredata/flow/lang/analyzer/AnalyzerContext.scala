package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.SchemaType

import scala.collection.mutable

/**
  * Propagate context
  *
  * @param database
  *   context database
  * @param catalog
  * @param parentAttributes
  *   attributes used in the parent relation. This is used for pruning unnecessary columns output attributes
  */
case class AnalyzerContext(scope: Scope):
  private val schemas = mutable.Map.empty[String, SchemaType]
  private val types   = mutable.Map.empty[String, DataType].addAll(DataType.knownPrimitiveTypes)

  private var _compileUnit: CompileUnit = CompileUnit.empty

  def compileUnit: CompileUnit = _compileUnit

  def getSchemas: Map[String, SchemaType] = schemas.toMap
  def getTypes: Map[String, DataType]     = types.toMap ++ schemas.toMap

  def addSchema(schema: SchemaType): Unit =
    schemas.put(schema.typeName, schema)

  def addType(dataType: DataType): Unit =
    types.put(dataType.typeName, dataType)

  def findSchema(name: String): Option[SchemaType] =
    schemas.get(name)

  def findType(name: String): Option[DataType] =
    types.get(name).orElse(findSchema(name))

  def withCompileUnit[U](newCompileUnit: CompileUnit)(block: AnalyzerContext => U): U =
    val prev = _compileUnit
    try
      _compileUnit = newCompileUnit
      block(this)
    finally _compileUnit = prev

/**
  * Scope of the context
  */
sealed trait Scope

object Scope:
  case object Global                    extends Scope
  case class Local(contextName: String) extends Scope
