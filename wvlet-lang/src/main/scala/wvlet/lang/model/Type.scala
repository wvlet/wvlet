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
package wvlet.lang.model

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.TypeName
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.plan.Import
import wvlet.log.LogSupport

abstract class Type:
  def typeDescription: String
  def isFunctionType: Boolean = false
  def isResolved: Boolean
  def isNotResolved: Boolean                          = !isResolved
  def isBound: Boolean                                = true
  def bind(typeArgMap: Map[TypeName, DataType]): Type =
    this match
      case d: DataType =>
        d
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Cannot bind type ${other}")

  def typeParams: Seq[DataType] = Nil

object Type:
  /**
    * Represents an untyped node (initial state before typing)
    */
  case object NoType extends Type:
    override def typeDescription: String = "NoType"
    override def isResolved: Boolean     = false

  /**
    * Represents a type error with an error message
    */
  case class ErrorType(msg: String) extends Type:
    override def typeDescription: String = s"ErrorType($msg)"
    override def isResolved: Boolean     = false

  /**
    * Represents the unit type (no value produced)
    */
  case object UnitType extends Type:
    override def typeDescription: String = "Unit"
    override def isResolved: Boolean     = true

  /**
    * Type variable for type inference (future use)
    */
  case class TypeVar(id: Int) extends Type:
    override def typeDescription: String = s"?T$id"
    override def isResolved: Boolean     = false

  case class ImportType(i: Import) extends Type:
    override def typeDescription: String = s"import ${i.importRef}"
    override def isResolved: Boolean     = true

  case class PackageType(name: Name) extends Type:
    override def typeDescription: String = s"package ${name}"
    override def isResolved: Boolean     = true

  abstract class LazyType extends (Symbol => LazyType):
    def apply(symbol: Symbol): LazyType

  case class FunctionType(
      name: Name,
      args: Seq[NamedType],
      returnType: DataType,
      contextNames: List[Name]
  ) extends Type
      with LogSupport:
    override def bind(typeArgMap: Map[TypeName, DataType]): FunctionType =
      val newArgs       = args.map(_.bind(typeArgMap))
      val newReturnType = returnType.bind(typeArgMap)
      val ft            = FunctionType(name, newArgs, newReturnType, contextNames)
      ft

    override def toString: String        = typeDescription
    override def typeDescription: String = s"${name}(${args.mkString(", ")}): ${returnType}"
    override def isFunctionType: Boolean = true
    override def isResolved: Boolean     = args.forall(_.isResolved) && returnType.isResolved

end Type
