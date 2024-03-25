package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{AliasedType, SchemaType}
import com.treasuredata.flow.lang.model.plan.TableDef
import wvlet.log.LogSupport

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
case class AnalyzerContext(scope: Scope) extends LogSupport:
  private val types    = mutable.Map.empty[String, DataType].addAll(DataType.knownPrimitiveTypes)
  private val aliases  = mutable.Map.empty[String, String]
  private val tableDef = mutable.Map.empty[String, TableDef]

  private var _compileUnit: CompileUnit = CompileUnit.empty

  def compileUnit: CompileUnit = _compileUnit

  def getAllTypes: Map[String, DataType]     = types.toMap
  def getAllTableDefs: Map[String, TableDef] = tableDef.toMap

  def addAlias(alias: String, typeName: String): Unit =
    aliases.put(alias, typeName)

  def addTableDef(tbl: TableDef): Unit =
    tableDef.put(tbl.name, tbl)

  def addType(dataType: DataType): Unit =
    trace(s"Add type: ${dataType.typeName}")
    types.put(dataType.typeName, dataType)

  def getTableDef(name: String): Option[TableDef] =
    tableDef.get(name)

  def resolveType(name: String, seen: Set[String] = Set.empty): Option[DataType] =
    if seen.contains(name) then None
    else
      findType(name).map(_.resolved) match
        case Some(r) =>
          if r.isResolved then Some(r)
          else resolveType(r.baseTypeName, seen + name)
        case other =>
          other

  def findType(name: String, seen: Set[String] = Set.empty): Option[DataType] =
    if seen.contains(name) then None
    val tpe = types
      .get(name)
      // search aliases
      .orElse(aliases.get(name).flatMap(types.get))
      // search table def
      .orElse {
        tableDef
          .get(name)
          .flatMap(_.getType)
          .flatMap(findType(_))
      }
    tpe

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
