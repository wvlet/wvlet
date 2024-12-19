package wvlet.lang.compiler

import wvlet.lang.api.{LinePosition, SourceLocation, Span}
import wvlet.log.LogSupport

/**
  * Handy method collection to extend Context class to resolve Source or Node locations in source
  * files
  */
object ContextUtil:

  extension (ctx: Context)
    def linePositionOf(span: Span): LinePosition =
      if span.isEmpty then
        LinePosition.NoPosition
      else
        val src  = ctx.compilationUnit.sourceFile
        val line = src.offsetToLine(span.start)
        val pos  = src.offsetToColumn(span.start)
        LinePosition(line + 1, pos)

    def endLinePositionOf(span: Span): LinePosition =
      val src  = ctx.compilationUnit.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      LinePosition(line + 1, pos)

    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(linePositionOf(span))

    def sourceLocationAt(nodeLocation: LinePosition): SourceLocation =
      val cu = ctx.compilationUnit
      cu.toSourceLocation(nodeLocation)

  extension (cu: CompilationUnit)
    def sourceLocationAt(span: Span): SourceLocation = sourceLocationAt(linePositionAt(span))
    def sourceLocationAt(nodeLocation: LinePosition): SourceLocation = cu
      .toSourceLocation(nodeLocation)

    def endLinePositionAt(span: Span): LinePosition =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.end)
      val pos  = src.offsetToColumn(span.end)
      LinePosition(line + 1, pos)

    def linePositionAt(span: Span): LinePosition =
      val src  = cu.sourceFile
      val line = src.offsetToLine(span.start)
      val pos  = src.offsetToColumn(span.start)
      LinePosition(line + 1, pos)

end ContextUtil

/**
  * Extend this method to add logging effective only when processing the context unit
  */
trait ContextLogSupport extends LogSupport:
  extension (c: Context)
    inline def logInfo(inline msg: => String): Unit =
      if c.isContextCompilationUnit then
        info(msg)

    inline def logWarn(inline msg: => String): Unit =
      if c.isContextCompilationUnit then
        warn(msg)

    inline def logDebug(inline msg: => String): Unit =
      if c.isContextCompilationUnit then
        debug(msg)

    inline def logTrace(inline msg: => String): Unit =
      if c.isContextCompilationUnit then
        trace(msg)
