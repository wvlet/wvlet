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
      resetNextToken()

  override protected def fetchToken(): Unit =
    initOffset()

    (ch: @switch) match
      case ' ' | '\t' | CR | LF | FF =>
        getWhiteSpaces()
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' |
          'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | '_' | 'a' | 'b' |
          'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' |
          'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' =>
        getIdentifier()
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '<' | '>' | '?' | ':' | '=' | '&' | '|' |
          '\\' =>
        getOperator()
      case '-' =>
        scanHyphen()
      case '0' =>
        scanZero()
      case '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        getNumber(base = 10)
      case '.' =>
        scanDot()
      case '\'' =>
        getSingleQuoteString()
      case '\"' =>
        getDoubleQuoteString(resultingToken = SqlToken.DOUBLE_QUOTED_IDENTIFIER)
      case '`' =>
        getBackQuoteString()
      case '/' =>
        scanSlash()
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
