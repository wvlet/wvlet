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

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.Type

/**
  * TyperState encapsulates typing-specific state that is carried through the typing phase. This
  * follows Scala 3's pattern of embedding typer state within Context.
  *
  * @param inputType
  *   The input relation type for typing expressions (e.g., columns available in scope)
  * @param errors
  *   Accumulated typing errors
  */
case class TyperState(
    inputType: RelationType = EmptyRelationType,
    private val errors: List[TyperError] = Nil
):

  /**
    * Set the input relation type for typing expressions
    */
  def withInputType(tpe: RelationType): TyperState = copy(inputType = tpe)

  /**
    * Set the input type from a generic Type
    */
  def withInputType(tpe: Type): TyperState =
    tpe match
      case rt: RelationType =>
        withInputType(rt)
      case _ =>
        this

  /**
    * Add a typing error
    */
  def addError(err: TyperError): TyperState = copy(errors = err :: errors)

  /**
    * Check if there are any typing errors
    */
  def hasErrors: Boolean = errors.nonEmpty

  /**
    * Get errors in order they were added
    */
  def errorsInOrder: List[TyperError] = errors.reverse

end TyperState

object TyperState:
  /**
    * Empty typer state with no input type and no errors
    */
  val empty: TyperState = TyperState()
