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

import wvlet.airframe.control.IO
import wvlet.lang.compiler.parser.Token.*
import wvlet.lang.model.SourceLocation
import wvlet.log.LogSupport

import scala.annotation.{switch, tailrec}
import scala.collection.mutable
import Token.*

// TODO Add line, offset
case class TokenData(token: Token, text: String, start: Int, end: Int):
  override def toString: String = f"[${start}%3d,${end}%3d] ${token}%10s: '${text}'"
  // TODO
  def getSourceLocation: Option[SourceLocation] = None

  class ScanState(startFrom: Int = 0):
    override def toString: String =
      s"ScanState(offset: ${offset}, lastOffset: ${lastOffset}, token: ${token}, str: ${str})"

    // Token type
    var token: Token = Token.EMPTY
    // The string value of the token
    var str: String = ""

    // The 1-character ahead offset of the last read character
    var offset: Int = startFrom
    // The offset of the character immediately before the current token
    var lastOffset: Int = startFrom
    // the offset of the newline immediately before the current token, or -1 if the current token is not the first one after a newline
    var lineOffset: Int = -1

class WvletScanner(source: ScannerSource) extends LogSupport:
  import WvletScanner.*

  // The last read character
  private var ch: Char = _
  // The 1-character ahead offset of the last read character
  private var offset: Int          = 0
  private var lineStartOffset: Int = 0

  // The offset before the last read character
  private var lastOffset: Int = 0

  private def isAfterLineEnd: Boolean = lineOffset >= 0

  /**
    */
  private var lineOffset: Int = -1
  protected val tokenBuffer   = TokenBuffer()

  // Token history
  private var prev = TokenData(Token.EMPTY, "", 0, 0)
  // One-lookahead token
  private var next = TokenData(Token.EMPTY, "", 0, 0)

  val currentRegion: Region = topLevelRegion(0)

  private def reportError(msg: String, loc: SourceLocation): Unit =
    error(s"${msg} at ${loc}")

  private def checkNoTrailingNumberSeparator(): Unit =
    if tokenBuffer.nonEmpty && isNumberSeparator(tokenBuffer.last) then
      reportError("trailing number separator", source.sourceLocationOf(offset))

  private def consume(expectedChar: Char): Unit =
    if ch != expectedChar then
      reportError(s"expected '${expectedChar}', but found '${ch}'", source.sourceLocationOf(offset))
    nextChar()

  // TODO Skip token until a safe location for error recovery
  def skipToken(): Unit = {}

  def nextToken(): TokenData =
    val token =
      if offset >= source.length then TokenData(Token.EOF, "", lastOffset, offset)
      else fetchToken()
    if isAfterLineEnd then handleNewLine()
    token

  private def indentWidth(offset: Int): Int =
    def loop(index: Int, ch: Char): Int =
      0
    loop(offset - 1, ' ')

  /**
    * Handle new lines. If necessary, add INDENT or OUTDENT tokens in front of the current token.
    *
    * Insert INDENT if
    *   - the indentation is significant, and
    *   - the last token can start an indentation region, and
    *   - the indentation of the current token is greater than the previous indentation width.
    */
  private def handleNewLine(): Unit =
    val indent = indentWidth(offset)
    debug(s"handle new line: ${offset}, indentWidth:${indent}")

  private def nextChar(): Unit =
    val index = offset
    lastOffset = index
    offset = index + 1
    if index >= source.length then ch = SU
    else
      ch = source.text.charAt(index)
      if ch < ' ' then fetchLineEnd()

  private def fetchLineEnd(): Unit =
    // skip CR
    if ch == CR then
      if offset < source.length && source.text.charAt(offset) == LF then
        offset += 1
        ch = LF
      else ch = LF

    if ch == LF || ch == FF then
      lineOffset = offset
      lineStartOffset = offset

  private def putChar(ch: Char): Unit =
    tokenBuffer.append(ch)

  private def fetchToken(): TokenData =
    // offset = charOffset - 1
    // lineOffset = if lastOffset < lineStartOffset then lineStartOffset else -1

    trace(s"fetchToken col:${offset - lineStartOffset} (offset:${offset}): '${String.valueOf(ch)}'")
    (ch: @switch) match
      case ' ' | '\t' | CR | LF | FF =>
        // Skip white space characters
        nextChar()
        fetchToken()
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' |
          'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | '_' | 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' |
          'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' =>
        putChar(ch)
        nextChar()
        getIdentRest()
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '-' | '>' | '?' | ':' | '=' | '&' | '|' | '\\' =>
        putChar(ch)
        nextChar()
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
          if base != 10 && !isNumberSeparator(ch) && digit2int(ch, base) < 0 then error("invalid literal number")
        fetchLeadingZero()
        getNumber(base)
      case '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        getNumber(base = 10)
      case ',' =>
        nextChar()
        TokenData(Token.COMMA, ",", offset - 1, offset)
      case '\"' =>
        getDoubleQuoteString()
      case SU =>
        TokenData(Token.EOF, "", offset, offset)
      case _ =>
        putChar(ch)
        nextChar()
        toToken()

  private def getDoubleQuoteString(): TokenData =
    // TODO Support unicode and escape characters
    consume('\"')
    while ch != '\"' && ch != SU do
      putChar(ch)
      nextChar()
    consume('\"')
    TokenData(Token.STRING_LITERAL, tokenBuffer.toString, offset - tokenBuffer.length, offset)

  @tailrec private def getOperatorRest(): TokenData =
    trace(s"getOperatorRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '-' | '<' | '>' | '?' | ':' | '=' | '&' | '|' | '\\' =>
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
        toToken()
      case _ =>
        toToken()
//      if isSpecial(ch) then {
//        putChar(ch); nextChar(); getOperatorRest()
//      }
//      else if isSupplementary(ch, isSpecial) then getOperatorRest()
//      else finishNamed()

  private def getIdentRest(): TokenData =
    trace(s"getIdentRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' |
          'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' |
          'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' | '0' |
          '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        putChar(ch)
        nextChar()
        getIdentRest()
      case _ =>
        val token = toToken()
        token
//    case '_' =>
//      putChar(ch)
//      nextChar()
//      getIdentOrOperatorRest()
//    case SU => // strangely enough, Character.isUnicodeIdentifierPart(SU) returns true!
//      finishNamed()
//    case _ =>
//      if isUnicodeIdentifierPart(ch) then
//        putChar(ch)
//        nextChar()
//        getIdentRest()
//      else if isSupplementary(ch, isUnicodeIdentifierPart) then
//        getIdentRest()
//      else
//        finishNamed()
  // def insertToken(token: Token, offset: Int): Unit = { ... }

  private def getAndClearTokenBuffer(): String =
    val str = tokenBuffer.toString
    tokenBuffer.clear()
    str

  private def toToken(): TokenData =
    val currentTokenStr = getAndClearTokenBuffer()
    trace(s"toToken at ${offset}: '${currentTokenStr}'")
    Token.keywordTable.get(currentTokenStr) match
      case Some(tokenType) =>
        TokenData(tokenType, currentTokenStr, offset - currentTokenStr.length, offset)
      case None =>
        TokenData(Token.IDENTIFIER, currentTokenStr, offset - currentTokenStr.length, offset)

  private def getNumber(base: Int): TokenData =
    while isNumberSeparator(ch) || digit2int(ch, base) >= 0 do
      putChar(ch)
      nextChar()
    checkNoTrailingNumberSeparator()
    var tokenType = Token.INTEGER_LITERAL

    if base == 10 && ch == '.' then
      putChar(ch)
      nextChar()
      if '0' <= ch && ch <= '9' then tokenType = getFraction()
    else
      (ch: @switch) match
        case 'e' | 'E' | 'f' | 'F' | 'd' | 'D' =>
          if base == 10 then tokenType = getFraction()
        case 'l' | 'L' =>
          nextChar()
          tokenType = Token.LONG_LITERAL
        case _ =>

    checkNoTrailingNumberSeparator()

    val strVal = getAndClearTokenBuffer()
    TokenData(tokenType, strVal, offset - strVal.length, offset)
  end getNumber

  private def getFraction(): Token =
    var tokenType = Token.DECIMAL_LITERAL
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
      tokenType = Token.EXP_LITERAL
    if ch == 'd' || ch == 'D' then
      putChar(ch)
      nextChar()
      tokenType = Token.DOUBLE_LITERAL
    else if ch == 'f' || ch == 'F' then
      putChar(ch)
      nextChar()
      tokenType = Token.FLOAT_LITERAL
    // checkNoLetter()
    tokenType
  end getFraction

object WvletScanner extends LogSupport:
  def scan(text: String): Seq[TokenData] =
    val scanner = new WvletScanner(StringSource(text))
    val tokens  = Seq.newBuilder[TokenData]
    scanner.nextChar()
    var tokenData = scanner.nextToken()
    while tokenData.token != Token.EOF do
      debug(s"token: ${tokenData}")
      tokens += tokenData
      tokenData = scanner.nextToken()
    tokens.result()

  /**
    * Convert a character to an integer value using the given base. Returns -1 upon failures
    */
  def digit2int(ch: Char, base: Int): Int =
    val num =
      if ch <= '9' then ch - '0'
      else if 'a' <= ch && ch <= 'z' then ch - 'a' + 10
      else if 'A' <= ch && ch <= 'Z' then ch - 'A' + 10
      else -1
    if 0 <= num && num < base then num else -1

  abstract class Region(val closeBy: Token):
    // The region enclonsing this region, or null if this is the outermost region
    def outer: Region | Null
    def enclosing: Region = outer.asInstanceOf[Region]
  end Region

  case class Indented(width: Int, outer: Region | Null) extends Region(OUTDENT)
  // For string-interpolation
  case class InString(outer: Region) extends Region(R_BRACE)
  case class InParens(outer: Region) extends Region(R_PAREN)
  case class InCase(outer: Region)   extends Region(OUTDENT)

  def topLevelRegion(width: Int) = Indented(width, null)
