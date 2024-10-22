package wvlet.lang.compiler

import wvlet.lang.api.{SourceLocation, Span, NodeLocation}

/**
  * Handy method collection to extend Context class to resolve Source or Node locations in source
  * files
  */
object ContextUtil:

  extension (ctx: Context)
    def nodeLocationOf(span: Span): NodeLocation =
      if span.isEmpty then
        NodeLocation.NoLocation
      else
        val src  = ctx.compilationUnit.sourceFile
        val line = src.offsetToLine(span.start)
        val pos  = src.offsetToColumn(span.start)
        NodeLocation(line + 1, pos)

    def endNodeLocationOf(span: Span): NodeLocation =
      val src  = ctx.compilationUnit.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      NodeLocation(line + 1, pos)

    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(nodeLocationOf(span))

    def sourceLocationAt(nodeLocation: NodeLocation): SourceLocation =
      val cu = ctx.compilationUnit
      cu.toSourceLocation(nodeLocation)

  extension (cu: CompilationUnit)
    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(nodeLocationAt(span))
    def sourceLocationAt(nodeLocation: NodeLocation): SourceLocation = cu
      .toSourceLocation(nodeLocation)

    def endNodeLocationAt(span: Span): NodeLocation =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      NodeLocation(line + 1, pos)

    def nodeLocationAt(span: Span): NodeLocation =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.start)
      val pos  = src.offsetToColumn(span.start)
      NodeLocation(line + 1, pos)

end ContextUtil
