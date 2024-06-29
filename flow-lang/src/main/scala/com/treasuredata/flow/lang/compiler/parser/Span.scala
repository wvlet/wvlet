package com.treasuredata.flow.lang.compiler.parser

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

  /**
    * Is this span different from NoSpan?
    */
  def exists: Boolean = this != Span.NoSpan

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
      Span(start, end, this.point - start)
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
