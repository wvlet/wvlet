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

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.NodeLocation

case class SourceLocation(compileUnit: CompilationUnit, nodeLocation: Option[NodeLocation]):
  def codeLineAt: String = nodeLocation
    .map { loc =>
      val line = compileUnit.sourceFile.sourceLine(loc.line)
      line
    }
    .getOrElse("")

  def locationString: String =
    nodeLocation match
      case Some(loc) =>
        s"${compileUnit.sourceFile.fileName}:${loc.line}:${loc.column}"
      case None =>
        compileUnit.sourceFile.relativeFilePath
