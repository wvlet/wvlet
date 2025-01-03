/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler.parser

import wvlet.lang.api.{LinePosition, SourceLocation, Span, StatusCode, WvletLangException}
import wvlet.lang.compiler.parser.WvletScanner.{
  InBackquoteString,
  InBraces,
  InString,
  Indented,
  Region
}
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.ContextUtil.*
import wvlet.log.LogSupport

import java.io.ObjectInputFilter.Status
import scala.annotation.{switch, tailrec}
import Tokens.*

/**
  * Scan *.wv files
  */
class WvletScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[WvletToken](sourceFile, config)
    with LogSupport:
  import WvletToken.*

  def nextToken(): TokenData[WvletToken] =
    val lastToken = current.token
    try
      getNextToken(lastToken)
      val t = current.toTokenData(lastCharOffset)
      if config.debugScanner then
        debug(s"${currentRegion} ${t}")
      t
    catch
      case e: WvletLangException
          if e.statusCode == StatusCode.UNEXPECTED_TOKEN && config.reportErrorToken =>
        current.token = WvletToken.ERROR
        currentRegion = Indented(0, null)
        val t = current.toTokenData(lastCharOffset)
        nextChar()
        t

  def inStringInterpolation: Boolean =
    currentRegion match
      case InString(_, _) =>
        true
      case _ =>
        false

  private def inMultiLineStringInterpolation: Boolean =
    currentRegion match
      case InBraces(InString(true, _)) =>
        true
      case _ =>
        false

  override protected def getNextToken(lastToken: WvletToken): Unit =
    // If the next token is already set, use it, otherwise fetch the next token
    if next.token == WvletToken.EMPTY then
      current.lastOffset = lastCharOffset
      currentRegion match
        case InString(multiline, _) =>
          getStringPart(multiline)
        case InBackquoteString(outer) =>
          getBackquotePart()
        case _ =>
          fetchToken()
    else
      current.copyFrom(next)
      next.token = WvletToken.EMPTY

  /**
    * Fetch the next token and set it to the current ScannerState
    */
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
            current.token = WvletToken.WHITESPACE
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
        if (ch == '"' || ch == '`') && current.token == WvletToken.IDENTIFIER then
          if ch == '`' then
            current.token = WvletToken.BACKQUOTE_INTERPOLATION_PREFIX
          else
            current.token = WvletToken.STRING_INTERPOLATION_PREFIX
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
      case '{' =>
        putChar(ch)
        nextChar()
        finishNamedToken()
      case '}' =>
        putChar(ch)
        if inMultiLineStringInterpolation then
          nextRawChar()
        else
          nextChar()
        currentRegion match
          case InBraces(outer) =>
            currentRegion = outer
          case _ =>
        finishNamedToken()
      case '\'' =>
        getSingleQuoteString()
      case '\"' =>
        getDoubleQuoteString()
      case '`' =>
        getBackQuoteString()
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
        current.token = WvletToken.EOF
        current.str = ""
      case _ =>
        putChar(ch)
        nextChar()
        finishNamedToken()

    end match

  end fetchToken

  private def getDoubleQuoteString(): Unit =
    if current.token == WvletToken.STRING_INTERPOLATION_PREFIX then
      currentRegion = InString(false, currentRegion)
      nextRawChar()
      if ch == '"' then
        if lookAheadChar() == '"' then
          nextRawChar()
          nextRawChar()
          // Triple quote strings
          getStringPart(multiline = true)
        else
          nextChar()
          // Empty string interpolation
          current.token = WvletToken.STRING_LITERAL
          current.str = flushTokenString()
      else
        // Single-line string interpolation
        getStringPart(multiline = false)
    else
      // Regular double quoted string
      // TODO Support unicode and escape characters
      nextChar()
      if ch == '\"' then
        nextChar()
        if ch == '\"' then
          nextRawChar()
          // Enter the triple-quote string
          getRawStringLiteral()
        else
          current.token = WvletToken.STRING_LITERAL
          current.str = ""
      else
        getStringLiteral()
  end getDoubleQuoteString

  private def getStringLiteral(): Unit =
    while ch != '"' && ch != SU do
      putChar(ch)
      nextChar()
    consume('"')
    current.token = WvletToken.STRING_LITERAL
    current.str = flushTokenString()

  private def getRawStringLiteral(): Unit =
    if ch == '\"' then
      nextRawChar()
      if isTripleQuote() then
        flushTokenString()
        current.token = WvletToken.STRING_LITERAL
      else
        getRawStringLiteral()
    else if ch == SU then
      reportError("Unclosed multi-line string literal", offset)
    else
      putChar(ch)
      nextRawChar()
      getRawStringLiteral()

  private def getStringPart(multiline: Boolean): Unit =
    (ch: @switch) match
      case '"' => // end of string
        if multiline then
          nextRawChar()
          if isTripleQuote() then
            flushTokenString()
            current.token = WvletToken.STRING_PART
          else
            getStringPart(multiline)
          // Exit the string interpolation scope
          if currentRegion.outer != null then
            currentRegion = currentRegion.outer
        else
          // Last part of the interpolated string
          nextChar()
          // Proceed a cursor
          current.offset += 1
          flushTokenString()
          current.token = STRING_PART
          currentRegion = currentRegion.outer
      case '\\' =>
        // escape character
        lookAheadChar() match
          case '$' | '"' =>
            nextRawChar() // skip '\'
            putChar(ch)
            nextRawChar()
          case _ =>
            putChar(ch)
            nextRawChar()
        getStringPart(multiline)
      case '$' =>
        lookAheadChar() match
          case '{' =>
            // Enter the in-string expression state
            flushTokenString()
            current.token = WvletToken.STRING_PART
            currentRegion = InBraces(currentRegion)
          case _ =>
            putChar(ch)
            nextChar()
            getStringPart(multiline)
      case SU =>
        reportError(s"unexpected end of file in string interpolation", offset)
      case _ =>
        putChar(ch)
        nextRawChar()
        getStringPart(multiline)

  private def isTripleQuote(): Boolean =
    if ch == '"' then
      nextRawChar()
      if ch == '"' then
        nextRawChar()
        while ch == '"' do
          putChar('"')
          nextChar()
        true
      else
        putChar('"')
        putChar('"')
        false
    else
      putChar('"')
      false

  private def getLineComment(): Unit =
    @tailrec
    def readToLineEnd(): Unit =
      putChar(ch)
      nextChar()
      if (ch != CR) && (ch != LF) && (ch != SU) then
        readToLineEnd()

    readToLineEnd()
    val commentLine = flushTokenString()
    if !config.skipComments then
      current.token = WvletToken.COMMENT
      current.str = commentLine
    else
      if config.debugScanner then
        debug(s"skip comment: ${commentLine}")
      fetchToken()

  private def getBlockComment(): Unit =
    @tailrec
    def readToCommentEnd(): Unit =
      putChar(ch)
      nextChar()
      if ch == '*' then
        putChar(ch)
        nextChar()
        if ch == '/' then
          putChar(ch)
          nextChar()
        else
          readToCommentEnd()
      else
        readToCommentEnd()

    readToCommentEnd()
    current.token = WvletToken.COMMENT
    current.str = flushTokenString()

  /**
    * Skip the comment and return true if the comment is skipped
    */
  private def skipComment(): Boolean =
    @tailrec
    def skipLine(): Unit =
      nextChar()
      if (ch != CR) && (ch != LF) && (ch != SU) then
        skipLine()

    // Skip `-- line comment`
    if ch == '-' then
      skipLine()
      true
    else
      false

  private def getSingleQuoteString(): Unit =
    consume('\'')
    while ch != '\'' && ch != SU do
      putChar(ch)
      nextChar()
    consume('\'')
    current.token = WvletToken.STRING_LITERAL
    current.str = flushTokenString()

  private def getBackQuoteString(): Unit =
    if current.token == BACKQUOTE_INTERPOLATION_PREFIX then
      currentRegion = InBackquoteString(currentRegion)
      nextRawChar()
      if ch == '`' then
        nextChar()
        // Empty string interpolation
        current.token = WvletToken.BACKQUOTED_IDENTIFIER
        current.str = flushTokenString()
      else
        // Single-line string interpolation
        getBackquotePart()
    else
      // Regular backquote string
      consume('`')
      while ch != '`' && ch != SU do
        putChar(ch)
        nextChar()
      consume('`')
      current.token = WvletToken.BACKQUOTED_IDENTIFIER
      current.str = flushTokenString()

  private def getBackquotePart(): Unit =
    (ch: @switch) match
      case '`' => // end of string
        // Last part of the interpolated string
        nextChar()
        // Proceed a cursor
        current.offset += 1
        flushTokenString()
        current.token = STRING_PART
        currentRegion = currentRegion.outer
      case '\\' =>
        // escape character
        lookAheadChar() match
          case '$' | '`' =>
            nextRawChar() // skip '\'
            putChar(ch)
            nextRawChar()
          case _ =>
            putChar(ch)
            nextRawChar()
        getBackquotePart()
      case '$' =>
        lookAheadChar() match
          case '{' =>
            // Enter the in-string expression state
            flushTokenString()
            current.token = WvletToken.STRING_PART
            currentRegion = InBraces(currentRegion)
          case _ =>
            putChar(ch)
            nextChar()
            getBackquotePart()
      case SU =>
        reportError(s"unexpected end of file in string interpolation", offset)
      case _ =>
        putChar(ch)
        nextRawChar()
        getBackquotePart()

  @tailrec
  private def getIdentRest(): Unit =
    trace(s"getIdentRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' |
          'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | 'a' | 'b' | 'c' |
          'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' |
          's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' | '0' | '1' | '2' | '3' | '4' | '5' | '6' |
          '7' | '8' | '9' | '_' =>
        putChar(ch)
        nextChar()
        getIdentRest()
      case _ =>
        finishNamedToken()

  @tailrec
  private def getOperatorRest(): Unit =
    trace(s"getOperatorRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case '~' | '!' | '@' | '#' | '%' | '^' | '+' | '-' | '<' | '>' | '?' | ':' | '=' | '&' | '|' |
          '\\' =>
        putChar(ch)
        nextChar()
        getOperatorRest()
      //    case '/' =>
      //      val nxch = lookAheadChar()
      //      if nxch == '/' || nxch == '*' then finishNamed()
      //      else {
      //        putChar(ch); nextChar(); getOperatorRest()
      //      }
      case SU =>
        finishNamedToken()
      case _ =>
        finishNamedToken()
  //      if isSpecial(ch) then {
  //        putChar(ch); nextChar(); getOperatorRest()
  //      }
  //      else if isSupplementary(ch, isSpecial) then getOperatorRest()
  //      else finishNamed()

  /**
    * Set the token string and clear the buffer
    */
  private def flushTokenString(): String =
    val str = tokenBuffer.toString
    current.str = str
    tokenBuffer.clear()
    str

  private def finishNamedToken(target: ScanState[WvletToken] = current): Unit =
    val currentTokenStr = flushTokenString()
    trace(s"finishNamedToken at ${current}: '${currentTokenStr}'")
    val token =
      WvletToken.keywordAndSymbolTable.get(currentTokenStr) match
        case Some(tokenType) =>
          target.token = tokenType
        case None =>
          target.token = WvletToken.IDENTIFIER

  private def getNumber(base: Int): Unit =
    while isNumberSeparator(ch) || digit2int(ch, base) >= 0 do
      putChar(ch)
      nextChar()
    checkNoTrailingNumberSeparator()
    var tokenType = WvletToken.INTEGER_LITERAL

    if base == 10 && ch == '.' then
      val nextCh = lookAheadChar()
      if '0' <= nextCh && nextCh <= '9' then
        putChar(ch)
        nextChar()
        tokenType = getFraction()
      else
        tokenType = WvletToken.INTEGER_LITERAL
    else
      (ch: @switch) match
        case 'e' | 'E' | 'f' | 'F' | 'd' | 'D' =>
          if base == 10 then
            tokenType = getFraction()
        case 'l' | 'L' =>
          nextChar()
          tokenType = WvletToken.LONG_LITERAL
        case _ =>

    checkNoTrailingNumberSeparator()

    flushTokenString()
    current.token = tokenType
  end getNumber

  private def getFraction(): WvletToken =
    var tokenType = WvletToken.DECIMAL_LITERAL
    trace(s"getFraction ch[${offset}]: '${ch}'")
    while '0' <= ch && ch <= '9' || isNumberSeparator(ch) do
      putChar(ch)
      nextChar()
    checkNoTrailingNumberSeparator()
    if ch == 'e' || ch == 'E' then
      putChar(ch)
      nextChar()
      if ch == '+' || ch == '-' then
        putChar(ch)
        nextChar()
      if '0' <= ch && ch <= '9' || isNumberSeparator(ch) then
        putChar(ch)
        nextChar()
        if ch == '+' || ch == '-' then
          putChar(ch)
          nextChar()
        while '0' <= ch && ch <= '9' || isNumberSeparator(ch) do
          putChar(ch)
          nextChar()
        checkNoTrailingNumberSeparator()
      tokenType = WvletToken.EXP_LITERAL
    if ch == 'd' || ch == 'D' then
      putChar(ch)
      nextChar()
      tokenType = WvletToken.DOUBLE_LITERAL
    else if ch == 'f' || ch == 'F' then
      putChar(ch)
      nextChar()
      tokenType = WvletToken.FLOAT_LITERAL
    // checkNoLetter()
    tokenType
  end getFraction

end WvletScanner

object WvletScanner:
  sealed trait Region:
    def outer: Region

  // Inside an interpolated string
  case class InString(multiline: Boolean, outer: Region) extends Region
  case class InBackquoteString(outer: Region)            extends Region
  case class InBraces(outer: Region)                     extends Region
  // Inside an indented region
  case class Indented(level: Int, outer: Region | Null) extends Region
