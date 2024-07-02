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

import wvlet.lang.compiler.ast.Tree

/**
  * Symbol is a permanent identifier for a variable, method, or class, etc.
  */
class Symbol(
    val id: Int,
):
  private var _defTree: Option[Tree] = None

  /**
    * The tree defining this symbol.
    * @return
    */
  def defTree: Option[Tree] = _defTree
  def defTree_=(tree: Tree): Unit =
    _defTree = Some(tree)

  def symbolInfo: SymbolInfo = ???



object Symbol:
  object NoSymbol extends Symbol(0)

  def newSymbol(
      owner: Symbol,
      name: Name,
  ): Symbol = {
    Symbol(owner, )
  }

  class ClassSymbol


abstract class SymbolInfo(symbol: Symbol, )
