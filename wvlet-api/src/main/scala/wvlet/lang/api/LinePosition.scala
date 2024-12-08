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
package wvlet.lang.api

/**
  * Line and column position in the source code
  * @param line
  * @param column
  */
case class LinePosition(
    // 1-origin line number
    line: Int,
    // column position in the line (1-origin)
    column: Int
):
  override def toString: String =
    if isEmpty then
      "?:?"
    else
      s"$line:$column"

  def isEmpty: Boolean  = line < 0
  def nonEmpty: Boolean = !isEmpty

  def map[U](f: LinePosition => U): Option[U] =
    if isEmpty then
      None
    else
      Some(f(this))

object LinePosition:
  val NoLocation: LinePosition = LinePosition(-1, 0)
