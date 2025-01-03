package wvlet.lang.compiler.parser

import wvlet.lang.api.{LinePosition, SourceLocation, Span}
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.parser.Tokens.*

import scala.annotation.switch

class SqlScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[SqlToken](sourceFile, config):

  override protected def getNextToken(lastToken: SqlToken): Unit =
    if next.token == SqlToken.EMPTY then
      current.lastOffset = lastCharOffset
      fetchToken()
    else
      current.copyFrom(next)
      next.token = SqlToken.EMPTY

  override protected def fetchToken(): Unit =
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
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' |
          'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | '_' | 'a' | 'b' |
          'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' |
          'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' =>
        putChar(ch)
        nextChar()
        getIdentRest()
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '<' | '>' | '?' | ':' | '=' | '&' | '|' |
          '\\' =>
        putChar(ch)
        nextChar()
        getOperatorRest()
      case '-' =>
        putChar(ch)
        nextChar()
        if ch == '-' then
          getLineComment()
        else if '0' <= ch && ch <= '9' then
          getNumber(base = 10)
        else
          getOperatorRest()
      case '0' =>
        var base: Int = 10
        def fetchLeadingZero(): Unit =
          putChar(ch)
          nextChar()
          ch match
            case 'x' | 'X' =>
              base = 16
              putChar(ch)
              nextChar()
            case _ =>
              base = 10
          if base != 10 && !isNumberSeparator(ch) && digit2int(ch, base) < 0 then
            error("invalid literal number")

        fetchLeadingZero()
        getNumber(base)
      case '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        getNumber(base = 10)
      case '.' =>
        nextChar()
        if '0' <= ch && ch <= '9' then
          putChar('.')
          getFraction()
          flushTokenString()
        else
          putChar('.')
          getOperatorRest()
      case '\'' =>
        getSingleQuoteString()
      case '\"' =>
        getDoubleQuoteString()
      case '/' =>
        putChar(ch)
        nextChar()
        ch match
          case '/' =>
            putChar(ch)
            nextChar()
            getOperatorRest()
          case '*' =>
            getBlockComment()
          case _ =>
            getOperatorRest()
      case SU =>
        current.token = SqlToken.EOF
        current.str = ""
      case _ =>
        putChar(ch)
        nextChar()
        finishNamedToken()
    end match

  end fetchToken

end SqlScanner
