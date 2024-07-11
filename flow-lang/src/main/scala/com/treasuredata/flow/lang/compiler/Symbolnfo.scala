package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.{DataType, Type}
import Type.PackageType
import com.treasuredata.flow.lang.model.plan.{EmptyRelation, LogicalPlan}

/**
  * SymbolInfo is the result of resolving a name (Symbol) during the compilation phase.
  *
  * @param symbol
  * @param owner
  * @param name
  * @param dataType
  */
class SymbolInfo(val symbol: Symbol, val name: Name, private var _tpe: Type):
  private var _declScope: Scope | Null = null
  def declScope_=(scope: Scope): Unit  = _declScope = scope
  def declScope: Scope                 = _declScope

  private var _plan: LogicalPlan | Null = null
  def plan: LogicalPlan                 = _plan
  def plan_=(p: LogicalPlan): Unit      = _plan = p

  def tpe: Type            = _tpe
  def tpe_=(t: Type): Unit = _tpe = t
  def dataType =
    tpe match
      case t: DataType =>
        t
      case _ =>
        DataType.UnknownType

  def dataType_=(d: DataType): Unit = tpe = d

class PackageSymbolInfo(symbol: Symbol, owner: Symbol, name: Name, tpe: PackageType, scope: Scope)
    extends SymbolInfo(symbol, name, tpe):
  this.declScope = scope

  override def toString: String =
    if owner.isNoSymbol then
      s"${name}"
    else
      s"${owner}.${name}"

/**
  * Maintains all types defined in the package
  */
class PackageTypeInfo

object NoSymbolInfo extends SymbolInfo(Symbol.NoSymbol, Name.NoName, Type.UnknownType):
  override def toString: String = "NoSymbol"

class NamedSymbolInfo(symbol: Symbol, owner: Symbol, name: Name, tpe: Type)
    extends SymbolInfo(symbol, name, tpe):
  override def toString: String = s"${owner}.${name}: ${dataType}"

class TypeSymbolInfo(symbol: Symbol, owner: Symbol, name: Name, tpe: DataType)
    extends NamedSymbolInfo(symbol, owner, name, tpe)

class ModelSymbolInfo(symbol: Symbol, owner: Symbol, name: Name, tpe: DataType)
    extends NamedSymbolInfo(symbol, owner, name, tpe):
  override def toString: String = s"model ${owner}.${name}: ${dataType}"

//class TypeInfo(symbol: Symbol, tpe: Type) extends SymbolInfo(symbol, tpe)
//
//case class SingleTypeInfo(symbol: Symbol, tpe: Type) extends TypeInfo(symbol, tpe)
//
//// Multiple types are used for overloaded methods
//case class MultipleTypeInfo(i1: TypeInfo, i2: TypeInfo)
//    extends TypeInfo(Symbol.NoSymbol, Type.UnknownType)
