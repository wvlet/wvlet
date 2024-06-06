package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.SourceFile

case class SourcePosition(source: SourceFile, span: Span):
  def line: Int   = source.offsetToLine(span.point)
  def column: Int = source.offsetToColumn(span.point)
