package wvlet.lang.compiler.parser

import wvlet.lang.api.{LinePosition, SourceLocation, Span}
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.parser.Tokens.*

import scala.annotation.switch

class SqlScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[SqlToken](sourceFile, config):

  override protected def getNextToken(lastToken: SqlToken): Unit =
    if next.token == SqlToken.EMPTY then
      fetchToken()
    else
      next.copyFrom(next)
      next.token = SqlToken.EMPTY

  private def fetchToken(): Unit =
    current.offset = charOffset - 1
    current.lineOffset =
      if current.lastOffset < lineStartOffset then
        lineStartOffset
      else
        -1
    trace(
      s"fetchToken[${current}]: '${String.valueOf(ch)}' charOffset:${charOffset} lastCharOffset:${lastCharOffset}, lineStartOffset:${lineStartOffset}"
    )
    (ch: @switch) match
      case ' ' | '\t' | CR | LF | FF =>
        def getWSRest(): Unit =
          if isWhiteSpaceChar(ch) then
            putChar(ch)
            nextChar()
            getWSRest()
          else
            current.token = SqlToken.WHITESPACE
            flushTokenString()

          if config.skipWhiteSpace then
            // Skip white space characters without pushing them into the buffer
            nextChar()
            fetchToken()
          else
            putChar(ch)
            nextChar()
            getWSRest()
      case _ =>

  end fetchToken

end SqlScanner
