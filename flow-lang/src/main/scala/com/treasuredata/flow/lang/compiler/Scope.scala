package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.plan.TableDef
import wvlet.log.LogSupport

import scala.collection.mutable

object Scope:
  def empty = Scope()

/**
  * Scope manages a list of table, alias, function definitions that are available in the current context.
  */
class Scope extends LogSupport:
  private val types    = mutable.Map.empty[String, DataType].addAll(DataType.knownPrimitiveTypes)
  private val aliases  = mutable.Map.empty[String, String]
  private val tableDef = mutable.Map.empty[String, TableDef]

  private var outer: Scope = null

  def getAllTypes: Map[String, DataType] = types.toMap

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
