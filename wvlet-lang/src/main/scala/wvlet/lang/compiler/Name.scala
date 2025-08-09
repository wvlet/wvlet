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

/**
  * Name represents a single leaf string value of term names (e.g., function name, variable, package
  * name) or type names (int, string, or user-defined types).
  *
  * Name is used for lookup entries in the symbol table (Scope) and Name needs to be created with
  * [[Name.termName]] or [[Name.typeName]] to ensure uniqueness of the name.
  *
  * The meaning of Name can be different depending on the current Scope. To resolve the meaning of
  * Name, its corresponding Symbol needs to be looked up in the current Scope, and resolve its
  * SymbolInfo.
  *
  *   - [[Name]] (string)
  *   - Search the current [[Scope]] (= SymbolTable) for the corresponding [[Symbol]]
  *   - From the [[Symbol]], check its [[SymbolInfo]] ([[DataType]], TypeDef, etc.), which will be
  *     refined as compiler phases proceed.
  */
sealed abstract class Name(val name: String) derives CanEqual:
  override def toString: String = name
  def isTermName: Boolean
  def isTypeName: Boolean
  def isEmpty: Boolean = this eq Name.NoName

  override def hashCode: Int              = System.identityHashCode(this)
  override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]
end Name

case class TermName private[compiler] (override val name: String) extends Name(name):
  private lazy val _typeName = TypeName(name)
  def toTypeName: TypeName   = _typeName

  def toSQLAttributeName: String =
    if name.matches("^[_a-zA-Z][_a-zA-Z0-9]*$") then
      name
    else if name.startsWith("\"") && name.endsWith("\"") then
      name
    else
      s""""${name}""""

  def toWvletAttributeName: String =
    if name.matches("^[_a-zA-Z][_a-zA-Z0-9]*$") then
      name
    else if name.startsWith("\"") && name.endsWith("\"") then
      s"`${name.substring(1, name.length - 1)}`"
    else
      s"`${name}`"

  override def isTermName: Boolean = true
  override def isTypeName: Boolean = false

case class TypeName private[compiler] (override val name: String) extends Name(name):
  override def isTermName: Boolean = false
  override def isTypeName: Boolean = true
  def toTermName: TermName         = Name.termName(name)

object TypeName:
  def of(s: String): TypeName = Name.typeName(s)

object TermName:
  def of(s: String): TermName = Name.termName(s)

object Name:
  // Manages a set of unique TermName objects
  private val nameTable = collection.mutable.Map.empty[String, TermName]

  val NoName     = termName("<NoName>")
  val NoTypeName = typeName("<NoType>")

  def termName(s: String): TermName = nameTable.getOrElseUpdate(s, TermName(s))
  def typeName(s: String): TypeName =
    // derive type name from term name
    termName(s).toTypeName
