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

/**
  * Representing reference or type names in the AST
  */
abstract class Name:
  def isTermName: Boolean
  def isTypeName: Boolean

object Name:
  case object NoName extends Name:
    def isTermName = false
    def isTypeName = false

  class TermName(nme: String) extends Name:
    def isTermName = true
    def isTypeName = false

  abstract class TypeName extends Name:
    def isTermName = false
    def isTypeName = true
  
  def termName(name: String): Name = TermName(name)

  final val WILDCARD = TermName("*")
