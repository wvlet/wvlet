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

import wvlet.lang.api.LinePosition
import wvlet.lang.api.SourceLocation
import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.parser.Scanner.InBackquoteString
import wvlet.lang.compiler.parser.Scanner.InBraces
import wvlet.lang.compiler.parser.Scanner.InString
import wvlet.lang.compiler.parser.Scanner.Indented
import wvlet.lang.compiler.parser.Scanner.Region
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceFile
import wvlet.lang.compiler.ContextUtil.*
import wvlet.log.LogSupport

import java.io.ObjectInputFilter.Status
import scala.annotation.switch
import scala.annotation.tailrec
import Tokens.*

/**
  * Scan *.wv files
  */
class WvletScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[WvletToken](sourceFile, config):
  import WvletToken.*

  def inStringInterpolation: Boolean =
    currentRegion match
      case InString(_, _) =>
        true
      case _ =>
        false

  /**
    * Peek ahead multiple tokens without consuming them. Returns an array of the next n tokens for
    * lookahead purposes. This creates a temporary scanner starting from the next token position.
    */
  def peekAhead(maxTokens: Int = 20): Array[TokenData[WvletToken]] =
    // Use next.offset if available (after lookAhead was called), otherwise use current.offset
    val startPos =
      if next.token != WvletToken.EMPTY then
        next.offset
      else
        current.offset
    val tempScanner = WvletScanner(
      sourceFile,
      config.copy(startFrom = startPos, skipComments = true)
    )
    val tokens = Array.newBuilder[TokenData[WvletToken]]
    var count  = 0
    while count < maxTokens && tempScanner.currentToken.token != WvletToken.EOF do
      tokens += tempScanner.nextToken()
      count += 1
    tokens.result()

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
      resetNextToken()

  /**
    * Fetch the next token and set it to the current ScannerState
    */
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
        if (ch == '"' || ch == '`') && current.token == WvletToken.IDENTIFIER then
          if ch == '`' then
            current.token = WvletToken.BACKQUOTE_INTERPOLATION_PREFIX
          else if lookAheadChar() == '"' && lookAheadChar(1) == '"' then
            current.token = WvletToken.TRIPLE_QUOTE_INTERPOLATION_PREFIX
          else
            current.token = WvletToken.STRING_INTERPOLATION_PREFIX
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
        scanSlash()
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
          current.token = WvletToken.DOUBLE_QUOTE_STRING
          current.str = flushTokenString()
      else
        // Single-line string interpolation
        getStringPart(multiline = false)
    else
      super.getDoubleQuoteString(WvletToken.DOUBLE_QUOTE_STRING)
  end getDoubleQuoteString

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

  override protected def getBackQuoteString(): Unit =
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
      super.getBackQuoteString()

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

end WvletScanner
