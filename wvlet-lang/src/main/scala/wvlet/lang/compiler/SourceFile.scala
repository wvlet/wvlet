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

import wvlet.lang.compiler.parser.{WvletScanner, WvletToken, SourcePosition, Span}
import wvlet.lang.compiler.parser.WvletToken.*
import wvlet.airframe.ulid.ULID
import wvlet.log.io.IOUtil

import java.io.File
import java.net.URL
import scala.collection.mutable.ArrayBuffer

object SourceFile:
  object NoSourceFile extends SourceFile(EmptyFile)
  def fromFile(v: VirtualFile): SourceFile = SourceFile(v)
  def fromFile(file: String): SourceFile   = SourceFile(LocalFile(file))
  def fromString(content: String): SourceFile = SourceFile(
    MemoryFile(s"${ULID.newULIDString}.wv", content)
  )

  def fromResource(url: URL): SourceFile     = SourceFile(URLResource(url))
  def fromResource(path: String): SourceFile = SourceFile(FileInResource(path))

class SourceFile(file: VirtualFile):
  override def toString: String = file.path

  /**
    * Returns the leaf file name
    * @return
    */
  def fileName: String               = new File(file.path).getName
  def relativeFilePath: String       = file.path
  def toCompileUnit: CompilationUnit = CompilationUnit(this)
  def lastUpdatedAt: Long            = file.lastUpdatedAt

  var content: IArray[Char]           = file.content
  private var lineIndexes: Array[Int] = computeLineIndexes

  def reload(): Unit =
    content = file.content
    lineIndexes = computeLineIndexes

  def length: Int = content.length

  private def computeLineIndexes: Array[Int] =
    val txt = content
    val buf = new ArrayBuffer[Int]
    buf += 0
    var i = 0
    while i < txt.length do
      val isLineBreak =
        val ch = txt(i)
        // CR LF
        if ch == CR then
          i + 1 == txt.length || content(i + 1) != LF
        else
          WvletToken.isLineBreakChar(ch)
      if isLineBreak then
        buf += i + 1
      i += 1
    buf += txt.length // sentinel, so that findLine below works smoother
    buf.toArray

  private def findLineIndex(offset: Int, hint: Int = -1): Int =
    val idx = java.util.Arrays.binarySearch(lineIndexes, offset)
    if idx >= 0 then
      idx
    else
      -idx - 2

  def sourcePositionAt(offset: Int): SourcePosition = SourcePosition(this, Span.at(offset))

  /**
    * 0-origin line index
    * @param offset
    * @return
    */
  def offsetToLine(offset: Int): Int = findLineIndex(offset)

  inline def charAt(pos: Int): Char = content(pos)

  def sourceLine(line: Int): String =
    val lineIndex = line - 1
    if lineIndex < 0 || lineIndex >= lineIndexes.length - 1 then
      ""
    else
      val start = lineIndexes(lineIndex)
      val end   = lineIndexes(lineIndex + 1) - 1
      if start <= end then
        content.slice(start, end).mkString
      else
        ""

  /**
    * Find the start of the line (offset) for the given offset
    */
  def startOfLine(offset: Int): Int =
    val lineStart = findLineIndex(offset)
    lineIndexes(lineStart)

  def nextLine(offset: Int): Int =
    val lineStart = findLineIndex(offset)
    lineIndexes(lineStart + 1)

  def offsetToColumn(offset: Int): Int =
    val lineStart = findLineIndex(offset)
    offset - lineIndexes(lineStart) + 1

end SourceFile
