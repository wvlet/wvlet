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

import wvlet.lang.StatusCode
import wvlet.log.LogSupport

object Scope:
  val NoScope = Scope(null, 0)

  /**
    * Create a new empty scope (e.g., for global package symbol)
    * @param nestingLevel
    * @return
    */
  def newScope(nestingLevel: Int): Scope = Scope(NoScope, nestingLevel)

case class ScopeEntry(name: Name, symbol: Symbol, owner: Scope)

/**
  * Scope holds a list of [[Symbol]]s that are available in the current context.
  */
class Scope(outer: Scope | Null, val nestingLevel: Int) extends LogSupport:
  // Initialize entries from the outer scope entries.
  // The outer scope might add more entries, but these new entries will not be visible from this Scope
  private var entries: List[ScopeEntry] =
    if outer != null then
      outer.getAllEntries
    else
      Nil

  def isEmpty: Boolean                = entries.isEmpty
  def getAllEntries: List[ScopeEntry] = entries

  /**
    * Get all symbols in this scope except outer symbols in the order they were entered.
    * @return
    */
  def getLocalSymbols: List[Symbol] = entries.filter(_.owner == this).map(_.symbol).reverse

  /**
    * Find all local symbols matching with the predicate
    * @param p
    * @return
    */
  def filter(p: Symbol => Boolean): List[Symbol] = entries
    .filter(e => e.owner == this)
    .filter(e => p(e.symbol))
    .map(_.symbol)

  def size: Int = entries.size

  def isNoScope: Boolean = this eq Scope.NoScope

  def lookupEntry(name: Name): Option[ScopeEntry] = entries.find(_.name == name)
  def lookupSymbol(name: Name): Option[Symbol]    = lookupEntry(name).map(_.symbol)

  /**
    * Register the symbol and its name if it is not already registered in the current scope.
    * @param sym
    * @param x$2
    * @return
    */
  def enter(sym: Symbol)(using Context): Symbol =
    val nme = sym.name
    lookupSymbol(nme) match
      case Some(sym) =>
        sym
      case None =>
        add(nme, sym)

  /**
    * Add a new mapping from the name to Symbol in the current scope
    * @param name
    * @param symbol
    * @return
    *   the entered symbol
    */
  def add(name: Name, symbol: Symbol): Symbol =
    if this.isNoScope then
      throw StatusCode
        .UNEXPECTED_STATE
        .newException(s"Cannot add an entry ${name} -> ${symbol.id} to NoScope")
    entries = ScopeEntry(name, symbol, this) :: entries
    symbol

  def newChildScope: Scope = Scope(outer = this, nestingLevel + 1)
  def newFreshScope: Scope = Scope(outer = null, nestingLevel + 1)
  def cloneScope: Scope    = Scope(outer = outer, nestingLevel)

end Scope
