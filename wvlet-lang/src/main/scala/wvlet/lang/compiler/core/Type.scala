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

class Type:
  /**
    * Type arguments for generic types
    * @return
    */
  def typeArgs: List[Type] = Nil
  def dealias: Type        = this

object Type:
  1.*(2)
  case object NoPrefix extends Type
  case object Unknown  extends Type

  // An alias to the underlying type
  case class TypeAlias(name: Name, underlying: Type) extends Type:
    override def dealias: Type = underlying

  // Lazy type for recursive type definition
  case class LazyType(name: Name) extends Type

  // Type with generic type arguments, such as Array[X], Map[K, V]
  case class GenericType(base: Type, override val typeArgs: List[Type]) extends Type

  case class ClassType(prefix: Type, name: Name, typeArgs: List[Type])           extends Type
  case class MethodType(name: Name, resultType: Type, argTypes: List[FieldType]) extends Type

  // A base trait for types representing a value
  trait DataType extends Type

  // A base trait for type parameters
  trait TypeParam extends DataType
  // Type variable for abstract types
  case class TypeVar(name: Name) extends TypeParam
  // Integer constant for varchar(n), decimal(p, q), etc.
  case class IntConstant(value: Int) extends TypeParam

  // A pair of name: Type, used for function arguments or record fields
  case class FieldType(name: Name, tpe: Type) extends DataType

  // A base type for relational data (table rows)
  case class RelationType(name: Name, fields: List[FieldType]) extends DataType
