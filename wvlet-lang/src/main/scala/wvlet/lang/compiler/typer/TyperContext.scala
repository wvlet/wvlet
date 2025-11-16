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

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Scope
import wvlet.lang.compiler.Symbol
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.lang.model.Type
import wvlet.lang.model.DataType.EmptyRelationType

/**
  * Context for the typer phase. This carries type information and scope through the typing process.
  */
case class TyperContext(
    owner: Symbol,
    scope: Scope,
    compilationUnit: CompilationUnit,
    context: Context,
    inputType: RelationType = EmptyRelationType,
    errors: List[TyperError] = Nil
):

  /**
    * Enter a new scope with the given owner
    */
  def withOwner(sym: Symbol): TyperContext = copy(owner = sym, scope = sym.symbolInfo.declScope)

  /**
    * Set the input relation type for typing expressions
    */
  def withInputType(tpe: RelationType): TyperContext = copy(inputType = tpe)

  /**
    * Set the input type from a generic Type
    */
  def withInputType(tpe: Type): TyperContext =
    tpe match
      case rt: RelationType =>
        withInputType(rt)
      case _ =>
        this

  /**
    * Look up a symbol by name in the current scope
    */
  def findSymbol(name: Name): Option[Symbol] = scope.lookupSymbol(name)

  /**
    * Add an error to the context
    */
  def addError(err: TyperError): TyperContext = copy(errors = err :: errors)

  /**
    * Check if typing has errors
    */
  def hasErrors: Boolean = errors.nonEmpty

end TyperContext

object TyperContext:
  /**
    * Create a TyperContext from a compilation context
    */
  def from(context: Context, unit: CompilationUnit): TyperContext = TyperContext(
    owner = context.owner,
    scope = context.scope,
    compilationUnit = unit,
    context = context
  )
