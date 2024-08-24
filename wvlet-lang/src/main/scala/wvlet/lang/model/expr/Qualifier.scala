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
package wvlet.lang.model.expr

//case class TableIdentifier(catalog: Option[String], database: Option[String], table: String)

class Qualifier(val parts: Seq[String]):
  def prefix: String = parts.mkString(".")
  def qualifiedName(name: String): String =
    if parts.isEmpty then
      name
    else
      s"${prefix}.$name"

  def isEmpty: Boolean               = parts.isEmpty
  def +(other: Qualifier): Qualifier = Qualifier(parts ++ other.parts)

object Qualifier:
  val empty = Qualifier(Seq.empty)

  def parse(s: String): Qualifier =
    s.split("\\.").toSeq match
      case s if s.isEmpty =>
        empty
      case parts =>
        Qualifier(parts)
