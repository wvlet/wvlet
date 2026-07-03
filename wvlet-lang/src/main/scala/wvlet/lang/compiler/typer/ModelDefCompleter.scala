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
package wvlet.lang.compiler.typer

import wvlet.lang.compiler.Context
import wvlet.lang.compiler.ModelSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.SymbolCompleter
import wvlet.lang.compiler.SymbolInfo
import wvlet.lang.model.plan.ModelDef

/**
  * Completes the SymbolInfo of a model definition on demand by typing the model body in the context
  * where the model was defined (following the lazy-completion design of the Scala 3 compiler's
  * Namer). This makes the resolved type of a model available to other compilation units regardless
  * of the order in which the units are compiled.
  *
  * @param m
  *   the model definition tree as parsed (before typing)
  * @param owner
  *   the owner symbol at the definition site (typically the package symbol)
  * @param name
  *   the term name of the model
  * @param definingContext
  *   the context at the definition site, carrying the defining compilation unit, its scope, and the
  *   imports preceding the definition
  */
class ModelDefCompleter(m: ModelDef, owner: Symbol, name: Name, definingContext: Context)
    extends SymbolCompleter:

  override def complete(symbol: Symbol): SymbolInfo =
    val typed = Typer.typeModelDef(m)(using definingContext)
    // Link the typed tree so that readers of symbol.tree see the resolved definition
    symbol.tree = typed
    ModelSymbolInfo(owner, symbol, name, typed.relationType, definingContext.compilationUnit)

end ModelDefCompleter
