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

import wvlet.lang.compiler.Context
import wvlet.lang.compiler.SourceLocation

case class NodeLocation(
    line: Int,
    // column position in the line (1-origin)
    column: Int
):
  override def toString: String = s"$line:$column"
  def toSourceLocation(using ctx: Context): SourceLocation = ctx
    .compilationUnit
    .toSourceLocation(Some(this))