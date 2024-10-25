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

import wvlet.lang.api.{NodeLocation, SourceLocation, Span}
import wvlet.lang.compiler.parser.{WvletScanner, WvletToken}
import wvlet.lang.compiler.parser.WvletToken.*
import wvlet.airframe.ulid.ULID
import wvlet.log.io.IOUtil

import java.io.File
import java.net.URL
import java.util.concurrent.atomic.AtomicBoolean
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

class SourceFile(val file: VirtualFile):
  def isEmpty: Boolean          = file eq EmptyFile
  override def toString: String = file.path

  /**
    * Returns the leaf file name
    * @return
    */
  def fileName: String               = file.name
  def relativeFilePath: String       = file.path
  def toCompileUnit: CompilationUnit = CompilationUnit(this)
  def lastUpdatedAt: Long            = file.lastUpdatedAt

  private val isLoaded                = AtomicBoolean(false)
  private var content: IArray[Char]   = IArray.emptyCharIArray
  private var lineIndexes: Array[Int] = Array.emptyIntArray

  def ensureLoaded: Unit = getContent

  def getContent: IArray[Char] =
    if isLoaded.compareAndSet(false, true) then
      loadContent
    content

  private def loadContent: IArray[Char] =
    content = file.content
    lineIndexes = computeLineIndexes
    content

  def reload(): Unit = isLoaded.set(false)

  def length: Int = getContent.length

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
    buf += txt.length + 1 // sentinel, so that findLine below works smoother
    buf.toArray

  private def findLineIndex(offset: Int, hint: Int = -1): Int =
    ensureLoaded
    val idx = java.util.Arrays.binarySearch(lineIndexes, offset)
    if idx >= 0 then
      idx
    else
      -idx - 2

  def sourceLocationAt(offset: Int): SourceLocation =
    val line   = offsetToLine(offset)
    val codeAt = sourceLine(line)
    SourceLocation(
      relativeFilePath,
      fileName,
      codeAt,
      NodeLocation(line + 1, offsetToColumn(offset))
    )

  /**
    * 0-origin line index
    * @param offset
    * @return
    */
  def offsetToLine(offset: Int): Int = findLineIndex(offset)

  inline def charAt(pos: Int): Char = content(pos)

  def sourceLine(line: Int): String =
    ensureLoaded
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
