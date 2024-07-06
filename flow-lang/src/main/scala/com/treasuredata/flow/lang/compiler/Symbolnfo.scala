package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.{DataType, Type}
import Type.PackageType

/**
  * SymbolInfo is the result of resolving a name (Symbol) during the compilation phase.
  *
  * @param symbol
  * @param owner
  * @param name
  * @param dataType
  */
class SymbolInfo(symbol: Symbol, val name: Name, val tpe: Type):
  private var _declScope: Scope | Null = null
  def declScope_=(scope: Scope): Unit  = _declScope = scope
  def declScope: Scope                 = _declScope

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
  override def toString: String = s"${owner}.${name}: ${tpe}"

case class TypeSymbolInfo(
    symbol: Symbol,
    owner: Symbol,
    override val name: Name,
    var dataType: DataType
) extends NamedSymbolInfo(symbol, owner, name, dataType)

//class TypeInfo(symbol: Symbol, tpe: Type) extends SymbolInfo(symbol, tpe)
//
//case class SingleTypeInfo(symbol: Symbol, tpe: Type) extends TypeInfo(symbol, tpe)
//
//// Multiple types are used for overloaded methods
//case class MultipleTypeInfo(i1: TypeInfo, i2: TypeInfo)
//    extends TypeInfo(Symbol.NoSymbol, Type.UnknownType)
