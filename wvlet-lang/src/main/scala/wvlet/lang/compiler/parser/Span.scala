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

import wvlet.lang.compiler.{Context, CompilationUnit, SourceLocation}
import wvlet.lang.model.NodeLocation

/**
  * Span is a range between start and end offset, and a point.
  * {{{
  *   start |-----------| end
  *             ^ (point)
  * }}}
  * Encoded as | 12 bit (pointDelta) | 26 bit (end) | 26 bit (start) |
  */
class Span(val coordinate: Long) extends AnyVal:
  override def toString: String =
    if exists then
      s"[${start}..${
          if point == start then
            ""
          else
            s"${point}.."
        }${end})"
    else
      "[NoSpan]"

  def map[U](f: Span => U): Option[U] =
    if exists then
      Some(f(this))
    else
      None

  def sourceLocation(using ctx: Context): SourceLocation =
    val cu = ctx.compilationUnit
    SourceLocation(cu, nodeLocation(using cu))

  def nodeLocation(using unit: CompilationUnit): NodeLocation =
    val src  = unit.sourceFile
    val line = src.offsetToLine(start)
    val pos  = src.offsetToColumn(start)
    NodeLocation(line + 1, pos)

  def endNodeLocation(using unit: CompilationUnit): NodeLocation =
    val src  = unit.sourceFile
    val line = src.offsetToLine(end)
    val pos  = src.offsetToColumn(end)
    NodeLocation(line + 1, pos)

  /**
    * Is this span different from NoSpan?
    */
  def exists: Boolean   = this != Span.NoSpan
  def isEmpty: Boolean  = !exists
  def nonEmpty: Boolean = exists

  def start: Int = (coordinate & Span.POSITION_MASK).toInt
  def end: Int   = ((coordinate >>> Span.POSITION_BITS) & Span.POSITION_MASK).toInt

  /**
    * The offset of the point from the start
    * @return
    */
  def pointOffset: Int = (coordinate >>> (Span.POSITION_BITS * 2)).toInt

  /**
    * The point of the span
    * @return
    */
  def point: Int = start + pointOffset

  def ==(other: Span): Boolean = coordinate == other.coordinate
  def !=(other: Span): Boolean = coordinate != other.coordinate

  def withStart(start: Int): Span =
    if exists then
      Span(start, end, this.point - start)
    else
      this

  def withEnd(end: Int): Span =
    if exists then
      Span(start, end, pointOffset)
    else
      this

  /**
    * Extend the span to the end of the given other span
    * @param other
    * @return
    */
  def extendTo(other: Span): Span =
    if exists then
      if other.exists && end < other.end then
        withEnd(other.end)
      else
        this
    else if other.exists then
      other
    else
      this

end Span

object Span:
  private inline val POSITION_BITS          = 26
  private inline val POSITION_MASK          = (1L << POSITION_BITS) - 1
  private inline val SYNTHETIC_POINT_OFFSET = 1 << (64 - POSITION_BITS * 2)

  /**
    * Non-existing span
    */
  val NoSpan: Span = within(1, 0)

  def at(offset: Int): Span              = within(offset, offset)
  def within(start: Int, end: Int): Span = apply(start, end, 0)
  def apply(start: Int, end: Int, pointDelta: Int): Span =
    val p = (pointDelta.toLong & POSITION_MASK).toLong
    new Span(
      (start & POSITION_MASK).toLong |
        (end & POSITION_MASK).toLong << POSITION_BITS |
        pointDelta.toLong <<
        (POSITION_BITS * 2)
    )
