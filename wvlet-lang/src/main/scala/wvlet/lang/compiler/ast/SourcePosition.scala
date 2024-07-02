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
package wvlet.lang.compiler.ast

import wvlet.lang.compiler.core.SourceFile
import wvlet.lang.model.SourceLocation

case class SourcePosition(source: SourceFile, offset: Int):
  def line: Int =
    var i    = 0
    var line = 1
    while i < offset do
      if source.text.charAt(i) == '\n' then line += 1
      i += 1
    line
  def column: Int =
    var i   = offset
    var col = 1
    while i > 0 && source.text.charAt(i) != '\n' do
      col += 1
      i -= 1
    col
  def toSourceLocation: SourceLocation = SourceLocation(line, column)
