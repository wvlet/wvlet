package com.treasuredata.flow.lang.compiler.parser

case class SourcePosition(source: ScannerSource, span: Span):
  def line: Int   = source.offsetToLine(span.point)
  def column: Int = source.offsetToColumn(span.point)
