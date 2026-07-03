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

import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.model.DataType
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.Type.ImportType
import wvlet.lang.model.Type.PackageType
import wvlet.lang.model.plan.Import
import wvlet.lang.model.plan.LogicalPlan
import wvlet.uni.log.LogSupport

/**
  * Computes the SymbolInfo of a symbol on first access (following the lazy-completion design of
  * Scala 3 compiler's Namer/LazyType). A completer is installed by SymbolLabeler when the
  * SymbolInfo cannot be computed reliably at labeling time (e.g., it requires typing the
  * definition), and captures whatever context it needs to complete later
  */
trait SymbolCompleter:
  def complete(symbol: Symbol): SymbolInfo

object Symbol:
  val NoSymbol: Symbol = Symbol(-1, Span.NoSpan)

  private val importName = Name.termName("<import>")

  def newPackageSymbol(owner: Symbol, name: Name)(using context: Context): Symbol =
    val symbol = Symbol(context.global.newSymbolId, Span.NoSpan)
    symbol.symbolInfo = PackageSymbolInfo(owner, symbol, name, PackageType(name), context.scope)
    symbol

  def newImportSymbol(owner: Symbol, i: Import)(using context: Context): Symbol =
    val symbol = Symbol(context.global.newSymbolId, i.span)
    symbol.symbolInfo = SymbolInfo(SymbolType.Import, NoSymbol, symbol, importName, ImportType(i))
    symbol

end Symbol

/**
  * Symbol is a permanent identifier for a TreeNode (e.g., LogicalPlan or Expression nodes). Symbol
  * holds the resolved SymbolInfo, which contains the DataType of the TreeNode.
  *
  * The SymbolInfo is either assigned directly (when it is known at symbol-creation time) or
  * computed lazily by a [[SymbolCompleter]] on the first access, so that definitions can be typed
  * on demand independently of the compilation order of their compilation units
  */
class Symbol(val id: Int, val span: Span) extends LogSupport:
  private var _name: Name                        = Name.NoName
  private var _symbolInfo: SymbolInfo | Null     = null
  private var _completer: SymbolCompleter | Null = null
  private var _isCompleting: Boolean             = false
  private var _tree: SyntaxTreeNode | Null       = null

  override def toString =
    if id == -1 then
      "NoSymbol"
    else if _name.isEmpty then
      s"Symbol($id)"
    else
      _name.name

  /**
    * The name of this symbol. The name is known eagerly (recorded when the SymbolInfo or a
    * completer is installed), so reading it never forces the completion of a lazy SymbolInfo
    */
  def name: Name = _name

  def isNoSymbol: Boolean = this == Symbol.NoSymbol

  def isDefined = !isNoSymbol
  def isEmpty   = !isDefined

  /**
    * The data type of this symbol. While the symbol's own completion is running (e.g., a
    * self-referencing definition), the type is not known yet, so UnknownType is returned instead of
    * forcing the completer again
    */
  def dataType: DataType =
    if _isCompleting then
      DataType.UnknownType
    else
      symbolInfo.dataType

  private def isResolved: Boolean = dataType.isResolved

  def isTypeSymbol: Boolean =
    this match
      case t: TypeSymbol =>
        true
      case _ =>
        false

  def isModelDef: Boolean =
    !_isCompleting && (
      symbolInfo match
        case m: ModelSymbolInfo =>
          true
        case _ =>
          false
    )

  def isRelationAlias: Boolean =
    !_isCompleting && (
      symbolInfo match
        case r: RelationAliasSymbolInfo =>
          true
        case _ =>
          false
    )

  def tree: SyntaxTreeNode =
    if _tree == null then
      LogicalPlan.empty
    else
      _tree

  def tree_=(t: SyntaxTreeNode): Unit =
    t match
      case l: LogicalPlan =>
        trace(s"Set Symbol(${id}) to ${l.pp}")
      case _ =>
    _tree = t

  /**
    * Returns true when this symbol already has a SymbolInfo or a completer that can produce one
    */
  def hasSymbolInfo: Boolean = _symbolInfo != null || _completer != null

  /**
    * Returns the SymbolInfo of this symbol, forcing the completer on the first access. Never
    * returns null: an uncompleted symbol without a completer yields [[NoSymbolInfo]]
    */
  def symbolInfo: SymbolInfo =
    if _symbolInfo != null then
      _symbolInfo
    else
      _completer match
        case null =>
          NoSymbolInfo
        case completer =>
          if _isCompleting then
            throw StatusCode
              .CYCLIC_SYMBOL_REFERENCE
              .newException(s"Cyclic reference detected while resolving symbol ${id}")
          _isCompleting = true
          try
            val info = completer.complete(this)
            _symbolInfo = info
            _completer = null
            info
          finally
            _isCompleting = false

  /**
    * Returns true while the completer of this symbol is running. Resolution code that may be
    * reached from within a completer (e.g., a recursive model reference) can check this to leave
    * the reference unresolved instead of triggering a cyclic-completion error
    */
  def isCompleting: Boolean = _isCompleting

  /**
    * Assign the SymbolInfo directly, discarding any pending completer. Used when the info is known
    * at creation time or when a definition is replaced (e.g., re-defining a model in REPL)
    */
  def symbolInfo_=(info: SymbolInfo): Unit =
    _symbolInfo = info
    _completer = null
    _name = info.name

  /**
    * Install a completer that computes the SymbolInfo on the first access, discarding any
    * previously assigned info (e.g., when a definition is replaced in REPL). The name is recorded
    * eagerly so that scope registration does not force the completion
    */
  def setCompleter(name: Name, completer: SymbolCompleter): Unit =
    _symbolInfo = null
    _completer = completer
    _name = name

end Symbol

case class TypeSymbol(override val id: Int, override val span: Span, sourceFile: SourceFile)
    extends Symbol(id, span)
