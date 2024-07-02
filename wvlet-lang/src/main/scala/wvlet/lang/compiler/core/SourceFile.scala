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

import wvlet.airframe.ulid.ULID
import wvlet.log.io.IOUtil

import java.io.File

object SourceFile:
  val fileExt: String = ".wv"

  object NoSourceFile extends SourceFile("<empty>", _ => "")
  def fromFile(file: String): SourceFile      = SourceFile(file, IOUtil.readAsString)
  def fromString(content: String): SourceFile = SourceFile(s"${ULID.newULIDString}${fileExt}", _ => content)
  def fromResource(path: String): SourceFile  = SourceFile(path, IOUtil.readAsString)

class SourceFile(val file: String, readContent: (file: String) => String):
  override def toString: String      = s"SourceFile($file)"
  def fileName: String               = new File(file).getName
  def toCompileUnit: CompilationUnit = CompilationUnit(this)
  lazy val text: String           = readContent(file)
