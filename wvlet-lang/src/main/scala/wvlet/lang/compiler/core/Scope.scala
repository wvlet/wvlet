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
package wvlet.lang.compiler.core

import Scope.*

/**
  * Scope contains a list of all symbols whose SymbolInfo varies during each phase of compilation.
  */
class Scope:
  def outer: Scope = this
  def nestingLevel: Int
  
  def toList: List[Symbol]

  def lookup(name: Name): Symbol =
    lookupEntry(name) match
      case Some(e) => e.symbol
      case None    => Symbol.NoSymbol
  def lookupEntry(name: Name): Option[ScopeEntry]

object Scope:
  object empty extends Scope:
    override def toList: List[Symbol]                        = Nil
    override def lookupEntry(name: Name): Option[ScopeEntry] = None

  class MutableScope(override val nestingLevel: Int) extends Scope:
    private var entries = List.empty[ScopeEntry]

    override def toList: List[Symbol] = entries.map(_.symbol)

    override def lookupEntry(name: Name): Option[ScopeEntry] = entries.find(_.name == name)

    def add(name: Name, symbol: Symbol): Symbol =
      val e = ScopeEntry(name, symbol, this)
      entries = e :: entries
      symbol

  case class ScopeEntry(name: Name, symbol: Symbol, owner: Scope)
