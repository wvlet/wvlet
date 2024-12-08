package wvlet.lang.compiler

import wvlet.lang.api.{SourceLocation, Span, LinePosition}

/**
  * Handy method collection to extend Context class to resolve Source or Node locations in source
  * files
  */
object ContextUtil:

  extension (ctx: Context)
    def nodeLocationOf(span: Span): LinePosition =
      if span.isEmpty then
        LinePosition.NoLocation
      else
        val src  = ctx.compilationUnit.sourceFile
        val line = src.offsetToLine(span.start)
        val pos  = src.offsetToColumn(span.start)
        LinePosition(line + 1, pos)

    def endNodeLocationOf(span: Span): LinePosition =
      val src  = ctx.compilationUnit.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      LinePosition(line + 1, pos)

    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(nodeLocationOf(span))

    def sourceLocationAt(nodeLocation: LinePosition): SourceLocation =
      val cu = ctx.compilationUnit
      cu.toSourceLocation(nodeLocation)

  extension (cu: CompilationUnit)
    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(nodeLocationAt(span))
    def sourceLocationAt(nodeLocation: LinePosition): SourceLocation = cu
      .toSourceLocation(nodeLocation)

    def endNodeLocationAt(span: Span): LinePosition =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      LinePosition(line + 1, pos)

    def nodeLocationAt(span: Span): LinePosition =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.start)
      val pos  = src.offsetToColumn(span.start)
      LinePosition(line + 1, pos)

end ContextUtil
