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
package wvlet.lang.compiler.parser

import wvlet.airframe.control.IO
import wvlet.lang.model.SourceLocation
import Token.*
import scala.collection.mutable.ArrayBuffer

trait ScannerSource:
  def text: String
  def length: Int

  private val lineIndexes: Array[Int] =
    val txt = text
    val buf = new ArrayBuffer[Int]
    buf += 0
    var i = 0
    while i < txt.length do
      val isLineBreak =
        val ch = txt(i)
        // CR LF
        if ch == CR then i + 1 == txt.length || text(i + 1) != LF
        else isLineBreakChar(ch)
      if isLineBreak then buf += i + 1
      i += 1
    buf += txt.length // sentinel, so that findLine below works smoother
    buf.toArray

  private def findLineIndex(offset: Int, hint: Int = -1): Int =
    val idx = java.util.Arrays.binarySearch(lineIndexes, offset)
    if idx >= 0 then idx
    else -idx - 2

  //def sourcePositionAt(offset: Int): SourcePosition = SourcePosition(this, Span.at(offset))
  def offsetToLine(offset: Int): Int                = findLineIndex(offset)

  inline def charAt(pos: Int): Char = text.charAt(pos)

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

object ScannerSource:
  def fromText(text: String): ScannerSource = StringSource(text)
  def fromFile(path: String): ScannerSource = SourceFile(path)

case class SourceFile(path: String) extends ScannerSource:
  override lazy val text: String = IO.readAsString(new java.io.File(path))
  override def length: Int       = text.length

case class StringSource(override val text: String) extends ScannerSource:
  override val length: Int = text.length