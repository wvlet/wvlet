package wvlet.lang.compiler.parser

import wvlet.lang.compiler.SourceFile

case class SourcePosition(source: SourceFile, span: Span):
  def line: Int   = source.offsetToLine(span.point)
  def column: Int = source.offsetToColumn(span.point)
