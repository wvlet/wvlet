/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler

import wvlet.lang.model.{DataType, TreeNode, Type}
import wvlet.lang.model.Type.PackageType
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

object Symbol:
  val NoSymbol: Symbol =
    new Symbol(-1):
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
class Symbol(val id: Int) extends LogSupport:
  private var _symbolInfo: SymbolInfo | Null = null
  private var _tree: TreeNode | Null         = null

  override def toString =
    if _symbolInfo == null then
      s"Symbol($id)"
    else
      _symbolInfo.toString

  def name: Name = symbolInfo.name

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

  def tree_=(t: TreeNode): Unit =
    t match
      case l: LogicalPlan =>
        trace(s"Set Symbol(${id}) to ${l.pp}")
      case _ =>
    _tree = t

  def symbolInfo: SymbolInfo =
//    if _symbolInfo == null then
//      _symbolInfo = computeSymbolInfo
    _symbolInfo

  def symbolInfo_=(info: SymbolInfo): Unit = _symbolInfo = info

  def computeSymbolInfo(using Context): SymbolInfo = ???

end Symbol

class TypeSymbol(id: Int, file: SourceFile) extends Symbol(id)
