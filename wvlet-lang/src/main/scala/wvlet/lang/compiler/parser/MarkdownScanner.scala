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

import wvlet.lang.compiler.SourceFile
import wvlet.log.LogSupport

import Tokens.*

/**
  * Scanner for Markdown files with blank line tracking
  * @param sourceFile
  * @param config
  */
class MarkdownScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[MarkdownToken](sourceFile, config)
    with LogSupport:
  import MarkdownToken.*

  // Track blank lines similar to comments in WvletParser
  private var blankLineBuffer: List[TokenData[MarkdownToken]] = Nil

  /**
    * Get the list of blank line tokens
    */
  def getBlankLineTokens(): List[TokenData[MarkdownToken]] = blankLineBuffer

  override protected def getNextToken(lastToken: MarkdownToken): Unit =
    initOffset()
    fetchToken()

  override protected def fetchToken(): Unit =
    initOffset()

    ch match
      case SU =>
        current.token = EOF
        current.str = ""
      case '\n' | '\r' =>
        scanNewline()
      case ' ' | '\t' =>
        getWhiteSpaces()
      case '#' if isAtLineStart =>
        scanHeading()
      case '`' =>
        scanBacktick()
      case '>' if isAtLineStart =>
        scanBlockquote()
      case '-' | '*' | '+' if isAtLineStart =>
        scanListOrHr()
      case '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' if isAtLineStart =>
        scanOrderedList()
      case _ =>
        scanText()

  private def isAtLineStart: Boolean = current.isAfterLineEnd

  /**
    * Scan newline and detect blank lines (consecutive newlines)
    */
  private def scanNewline(): Unit =
    putChar(ch)
    nextChar()
    current.token = NEWLINE
    current.str = flushTokenString()

    // Check if the next line is also empty (= blank line)
    if ch == '\n' || ch == '\r' then
      // Found a blank line
      val blankLineToken = current.toTokenData(lastCharOffset)
      blankLineBuffer = blankLineToken :: blankLineBuffer
      if config.debugScanner then
        debug(s"blank line detected: ${blankLineToken}")

  /**
    * Scan heading (# characters at line start)
    */
  private def scanHeading(): Unit =
    var level = 0
    while ch == '#' && level < 6 do
      putChar(ch)
      nextChar()
      level += 1

    // Skip whitespace after #
    while ch == ' ' || ch == '\t' do
      nextChar()

    // Read heading text until end of line
    while ch != '\n' && ch != '\r' && ch != SU do
      putChar(ch)
      nextChar()

    current.token = HEADING
    current.str = flushTokenString()

  /**
    * Scan backticks (code span or fence)
    */
  private def scanBacktick(): Unit =
    var count = 0
    while ch == '`' && count < 3 do
      putChar(ch)
      nextChar()
      count += 1

    if count == 3 then
      // Code fence
      // Read optional language hint
      while ch != '\n' && ch != '\r' && ch != SU do
        putChar(ch)
        nextChar()
      current.token = FENCE
    else
      // Code span (inline code)
      // Read until closing backtick
      while ch != '`' && ch != '\n' && ch != '\r' && ch != SU do
        putChar(ch)
        nextChar()
      if ch == '`' then
        putChar(ch)
        nextChar()
      current.token = CODE_SPAN

    current.str = flushTokenString()

  /**
    * Scan blockquote marker (> at line start)
    */
  private def scanBlockquote(): Unit =
    putChar(ch)
    nextChar()

    // Skip optional space after >
    if ch == ' ' || ch == '\t' then
      nextChar()

    // Read rest of line
    while ch != '\n' && ch != '\r' && ch != SU do
      putChar(ch)
      nextChar()

    current.token = BLOCKQUOTE
    current.str = flushTokenString()

  /**
    * Scan list marker or horizontal rule
    */
  private def scanListOrHr(): Unit =
    val marker = ch
    putChar(ch)
    nextChar()

    // Check for horizontal rule (---, ***, ___)
    if (marker == '-' || marker == '*' || marker == '_') && (ch == marker || ch == ' ') then
      var count = 1
      while ch == marker || ch == ' ' || ch == '\t' do
        if ch == marker then
          count += 1
        putChar(ch)
        nextChar()

      if count >= 3 && (ch == '\n' || ch == '\r' || ch == SU) then
        current.token = HR
        current.str = flushTokenString()
        return

    // List marker
    // Skip whitespace after marker
    if ch == ' ' || ch == '\t' then
      nextChar()

    // Read rest of line
    while ch != '\n' && ch != '\r' && ch != SU do
      putChar(ch)
      nextChar()

    current.token = LIST_MARKER
    current.str = flushTokenString()

  end scanListOrHr

  /**
    * Scan ordered list (1. 2. etc at line start)
    */
  private def scanOrderedList(): Unit =
    // Read digits
    while ch >= '0' && ch <= '9' do
      putChar(ch)
      nextChar()

    // Check for period
    if ch == '.' then
      putChar(ch)
      nextChar()

      // Skip whitespace
      if ch == ' ' || ch == '\t' then
        nextChar()

      // Read rest of line
      while ch != '\n' && ch != '\r' && ch != SU do
        putChar(ch)
        nextChar()

      current.token = LIST_MARKER
      current.str = flushTokenString()
    else
      // Not a list, treat as text
      scanText()

  /**
    * Scan regular text until special character or newline
    */
  private def scanText(): Unit =
    while ch != '\n' && ch != '\r' && ch != SU && ch != '#' && ch != '`' && ch != '>' &&
      ch != '-' && ch != '*' && ch != '+'
    do
      putChar(ch)
      nextChar()

    if charOffset > offset then
      current.token = TEXT
      current.str = flushTokenString()
    else
      // No text consumed, skip character
      putChar(ch)
      nextChar()
      current.token = TEXT
      current.str = flushTokenString()

  override protected def getWhiteSpaces(): Unit =
    while ch == ' ' || ch == '\t' do
      putChar(ch)
      nextChar()
    current.token = WHITESPACE
    current.str = flushTokenString()

end MarkdownScanner
