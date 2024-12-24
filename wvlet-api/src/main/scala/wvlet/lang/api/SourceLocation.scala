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

case class SourceLocation(
    path: String,
    fileName: String,
    // The line of the source code where the error occurred
    codeLineAt: String,
    position: LinePosition
):
  override def toString: String = locationString
  def lineLocationString: String =
    if position.isEmpty then
      path
    else
      s"${fileName}:${position.line}"

  def locationString: String =
    if position.isEmpty then
      path
    else
      s"${fileName}:${position.line}:${position.column}"

object SourceLocation:
  val NoSourceLocation = SourceLocation("", "", "", LinePosition.NoPosition)
