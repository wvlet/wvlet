package com.treasuredata.flow.lang.analyzer

import com.treasuredata.flow.lang.CompileUnit
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.SchemaType
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

  def getTypes: Map[String, DataType] = types.toMap

  def addAlias(alias: String, typeName: String): Unit =
    aliases.put(alias, typeName)

  def addTableDef(tbl: TableDef): Unit =
    tableDef.put(tbl.name, tbl)

  def addType(dataType: DataType): Unit =
    types.put(dataType.typeName, dataType)

  def findType(name: String): Option[DataType] =
    types
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
