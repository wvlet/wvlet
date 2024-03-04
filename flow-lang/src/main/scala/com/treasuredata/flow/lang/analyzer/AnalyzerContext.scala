package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.analyzer.Type.{RecordType, knownPrimitiveTypes}
import com.treasuredata.flow.lang.model.DataType

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
  private val schemas = mutable.Map.empty[String, RecordType]
  private val types   = mutable.Map.empty[String, Type].addAll(knownPrimitiveTypes.iterator)

  private var _compileUnit: CompileUnit = CompileUnit.empty

  def compileUnit: CompileUnit = _compileUnit

  def getSchemas: Map[String, RecordType] = schemas.toMap
  def getTypes: Map[String, Type]         = types.toMap ++ schemas.toMap

  def addSchema(schema: RecordType): Unit =
    schemas.put(schema.typeName, schema)

  def addType(dataType: Type): Unit =
    types.put(dataType.typeName, dataType)

  def findSchema(name: String): Option[RecordType] =
    schemas.get(name)

  def findType(name: String): Option[Type] =
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

sealed trait Type:
  def typeName: String
  def isResolved: Boolean

object Type:
  case class ResolvedType(dataType: DataType) extends Type:
    override def typeName: String    = dataType.typeName
    override def isResolved: Boolean = true

  case class UnresolvedType(typeName: String) extends Type:
    override def isResolved: Boolean = false

  case class NamedType(name: String, tpe: Type) extends Type:
    override def typeName: String    = tpe.typeName
    override def isResolved: Boolean = tpe.isResolved

  case class ExtensionType(typeName: String, selfType: NamedType, defs: Seq[Def]) extends Type:
    override def isResolved: Boolean = selfType.isResolved && defs.forall(_.isResolved)

  case class RecordType(typeName: String, typeDefs: Seq[NamedType]) extends Type:
    override def isResolved: Boolean = typeDefs.forall(_.tpe.isResolved)

  val knownPrimitiveTypes: Map[String, Type] = DataType.getPrimitiveTypeTable.map { case (name, dataType) =>
    name -> ResolvedType(dataType)
  }

sealed trait Def:
  def isResolved: Boolean

object Def:
  case class FunctionDef(name: String, args: Seq[Type.NamedType], returnType: Type, body: Block) extends Def:
    override def isResolved: Boolean = args.forall(_.tpe.isResolved) && returnType.isResolved

// Function body block
trait Block
