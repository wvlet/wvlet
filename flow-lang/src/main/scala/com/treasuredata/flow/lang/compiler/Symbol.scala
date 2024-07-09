package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.{DataType, TreeNode, Type}
import com.treasuredata.flow.lang.model.Type.PackageType
import com.treasuredata.flow.lang.model.expr.NameExpr
import com.treasuredata.flow.lang.model.expr.NameExpr.EmptyName
import com.treasuredata.flow.lang.model.plan.LogicalPlan

object Symbol:
  val NoSymbol: Symbol =
    new Symbol(0):
      override def computeSymbolInfo(using Context): SymbolInfo = NoSymbolInfo

  private val importName = Name.termName("<import>")
//  lazy val EmptyPackage: Symbol = newPackageSymbol(rootPackageName)
//  lazy val RootType: Symbol = newTypeDefSymbol(NoSymbol, NoName, DataType.UnknownType)
//
  def newPackageSymbol(owner: Symbol, name: Name)(using context: Context): Symbol =
    val symbol = Symbol(context.global.newSymbolId)
    symbol.symbolInfo = PackageSymbolInfo(symbol, owner, name, PackageType(name), context.scope)
    symbol

  def newImportSymbol(owner: Symbol, tpe: Type)(using context: Context): Symbol =
    val symbol = Symbol(context.global.newSymbolId)
    symbol.symbolInfo = NamedSymbolInfo(symbol, NoSymbol, importName, tpe)
    symbol

end Symbol

/**
  * Symbol is a permanent identifier for a TreeNode of LogicalPlan or Expression nodes. Symbol holds
  * a cache of the resolved SymbolInfo, which contains DataType for the TreeNode.
  *
  * @param name
  */
class Symbol(val id: Int):
  private var _symbolInfo: SymbolInfo | Null = null
  private var _tree: TreeNode | Null         = null

  override def toString =
    if _symbolInfo == null then
      s"Symbol($id)"
    else
      _symbolInfo.toString

  def name(using Context): Name = symbolInfo.name

  def isNoSymbol: Boolean = this == Symbol.NoSymbol

  def dataType: DataType =
    if _symbolInfo == null then
      DataType.UnknownType
    else
      _symbolInfo.dataType

  private def isResolved: Boolean = dataType.isResolved

  def tree: TreeNode =
    if _tree == null then
      LogicalPlan.empty
    else
      _tree

  def tree_=(t: TreeNode): Unit = _tree = t

  def symbolInfo(using Context): SymbolInfo =
    if _symbolInfo == null then
      _symbolInfo = computeSymbolInfo

    _symbolInfo

  def symbolInfo_=(info: SymbolInfo): Unit = _symbolInfo = info

  def computeSymbolInfo(using Context): SymbolInfo = ???

end Symbol

class TypeSymbol(id: Int, file: SourceFile) extends Symbol(id)
