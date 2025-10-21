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

import scala.annotation.switch

/**
  * Scanner for Markdown files following CommonMark specification Implements line-based scanning for
  * block elements and character-level scanning for inline elements
  */
class MarkdownScanner(sourceFile: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase[MarkdownToken](sourceFile, config):
  import MarkdownToken.*
  import Tokens.*

  private var isLineStart: Boolean      = true
  private var inCodeBlock: Boolean      = false
  private var codeBlockFenceLength: Int = 0

  override protected def getNextToken(lastToken: MarkdownToken): Unit =
    if next.token == MarkdownToken.EMPTY then
      current.lastOffset = lastCharOffset
      fetchToken()
    else
      current.copyFrom(next)
      resetNextToken()

  override protected def fetchToken(): Unit =
    initOffset()

    // Handle code block mode separately
    if inCodeBlock then
      scanCodeBlockContent()
      return

    (ch: @switch) match
      case ' ' | '\t' | CR | LF | FF =>
        scanWhiteSpaces()
        if ch == LF || ch == CR then
          isLineStart = true
      case '#' if isLineStart =>
        scanHeading()
      case '`' =>
        scanBacktick()
      case '*' =>
        scanAsterisk()
      case '_' =>
        scanUnderscore()
      case '-' if isLineStart =>
        scanDashAtLineStart()
      case '+' if isLineStart =>
        scanListBullet(MarkdownToken.PLUS)
      case '>' if isLineStart =>
        scanBlockquote()
      case '[' =>
        scanLinkOrImage(isImage = false)
      case '!' if lookAheadChar() == '[' =>
        scanLinkOrImage(isImage = true)
      case SU =>
        current.token = MarkdownToken.EOF
        current.str = ""
      case _ =>
        scanText()

    end match

    // After consuming a token, we're no longer at line start (unless it was whitespace)
    if current.token != MarkdownToken.WHITESPACE && current.token != MarkdownToken.NEWLINE then
      isLineStart = false

  end fetchToken

  /**
    * Scan heading: # through ###### at line start
    */
  private def scanHeading(): Unit =
    var level = 0
    while ch == '#' && level < 6 do
      putChar(ch)
      nextChar()
      level += 1

    // Must be followed by space or end of line
    if ch == ' ' || ch == '\t' || ch == LF || ch == CR || ch == SU then
      // Skip whitespace after hash marks
      while ch == ' ' || ch == '\t' do
        nextChar()

      // Read the heading text until end of line
      while ch != LF && ch != CR && ch != SU do
        putChar(ch)
        nextChar()

      current.token = MarkdownToken.HEADING
      current.str = flushTokenString()
    else
      // Not a valid heading, treat as text
      while ch != ' ' && ch != '\t' && ch != LF && ch != CR && ch != SU do
        putChar(ch)
        nextChar()
      current.token = MarkdownToken.TEXT
      current.str = flushTokenString()

  /**
    * Scan backticks for code blocks (```) or inline code (`)
    */
  private def scanBacktick(): Unit =
    var count = 0
    while ch == '`' do
      putChar(ch)
      nextChar()
      count += 1

    if count >= 3 && isLineStart then
      // Code fence
      inCodeBlock = true
      codeBlockFenceLength = count

      // Read optional language hint
      while ch == ' ' || ch == '\t' do
        putChar(ch)
        nextChar()

      while ch != LF && ch != CR && ch != SU do
        putChar(ch)
        nextChar()

      current.token = MarkdownToken.FENCE_MARKER
      current.str = flushTokenString()
    else if count == 1 then
      // Inline code span
      scanCodeSpan()
    else
      // Just backticks as text
      current.token = MarkdownToken.TEXT
      current.str = flushTokenString()

  end scanBacktick

  /**
    * Scan inline code span: `code`
    */
  private def scanCodeSpan(): Unit =
    // Already consumed opening backtick
    while ch != '`' && ch != LF && ch != CR && ch != SU do
      putChar(ch)
      nextChar()

    if ch == '`' then
      nextChar() // consume closing backtick

    current.token = MarkdownToken.CODE_SPAN
    current.str = flushTokenString()

  /**
    * Scan code block content until closing fence
    */
  private def scanCodeBlockContent(): Unit =
    // Check if we're at a potential closing fence
    if isLineStart && ch == '`' then
      var count       = 0
      val startOffset = charOffset
      while ch == '`' do
        count += 1
        nextChar()

      if count >= codeBlockFenceLength then
        // Closing fence found
        inCodeBlock = false
        codeBlockFenceLength = 0

        // Consume rest of line
        while ch != LF && ch != CR && ch != SU do
          nextChar()

        current.token = MarkdownToken.FENCE_MARKER
        current.str = "```"
        return
      else
        // Not a closing fence, continue reading content
        // Reset to start and read as normal content
        charOffset = startOffset
        ch = buf(charOffset - 1)

    // Read code block content line by line
    while ch != LF && ch != CR && ch != SU do
      putChar(ch)
      nextChar()

    if ch == LF || ch == CR then
      putChar(ch)
      nextChar()
      isLineStart = true

    current.token = MarkdownToken.CODE_BLOCK
    current.str = flushTokenString()

  end scanCodeBlockContent

  /**
    * Scan asterisk for bold/italic or list bullet
    */
  private def scanAsterisk(): Unit =
    if isLineStart then
      // Could be a list bullet
      putChar(ch)
      nextChar()
      if ch == ' ' || ch == '\t' then
        current.token = MarkdownToken.LIST_ITEM
        current.str = flushTokenString()
      else
        scanEmphasis('*')
    else
      scanEmphasis('*')

  /**
    * Scan underscore for bold/italic
    */
  private def scanUnderscore(): Unit = scanEmphasis('_')

  /**
    * Scan emphasis markers (** or __ for bold, * or _ for italic)
    */
  private def scanEmphasis(marker: Char): Unit =
    var count = 0
    while ch == marker do
      putChar(ch)
      nextChar()
      count += 1

    if count >= 2 then
      // Bold
      while ch != marker && ch != LF && ch != CR && ch != SU do
        putChar(ch)
        nextChar()

      // Try to consume closing markers
      var closingCount = 0
      while ch == marker && closingCount < 2 do
        nextChar()
        closingCount += 1

      current.token = MarkdownToken.BOLD
      current.str = flushTokenString()
    else if count == 1 then
      // Italic
      while ch != marker && ch != LF && ch != CR && ch != SU do
        putChar(ch)
        nextChar()

      if ch == marker then
        nextChar()

      current.token = MarkdownToken.ITALIC
      current.str = flushTokenString()
    else
      current.token = MarkdownToken.TEXT
      current.str = flushTokenString()

  end scanEmphasis

  /**
    * Scan dash at line start: could be list item, horizontal rule, or just text
    */
  private def scanDashAtLineStart(): Unit =
    var dashCount = 0
    val startPos  = charOffset

    while ch == '-' do
      dashCount += 1
      nextChar()

    if dashCount >= 3 &&
      (ch == ' ' || ch == '\t' || ch == LF || ch == CR || ch == SU)
    then
      // Horizontal rule
      current.token = MarkdownToken.HORIZONTAL_RULE
      current.str = "---"
    else if dashCount == 1 && (ch == ' ' || ch == '\t') then
      // List bullet
      current.token = MarkdownToken.LIST_ITEM
      current.str = "-"
    else
      // Regular text with dashes
      charOffset = startPos
      ch = buf(charOffset - 1)
      scanText()

  /**
    * Scan list bullet (+ or *)
    */
  private def scanListBullet(tokenType: MarkdownToken): Unit =
    putChar(ch)
    nextChar()

    if ch == ' ' || ch == '\t' then
      current.token = MarkdownToken.LIST_ITEM
      current.str = flushTokenString()
    else
      scanText()

  /**
    * Scan blockquote marker (>)
    */
  private def scanBlockquote(): Unit =
    putChar(ch)
    nextChar()

    if ch == ' ' || ch == '\t' || ch == LF || ch == CR then
      current.token = MarkdownToken.BLOCKQUOTE
      current.str = flushTokenString()
    else
      scanText()

  /**
    * Scan link [text](url) or image ![alt](url)
    */
  private def scanLinkOrImage(isImage: Boolean): Unit =
    if isImage then
      putChar(ch) // !
      nextChar()

    putChar(ch) // [
    nextChar()

    // Read link text/alt text
    while ch != ']' && ch != LF && ch != CR && ch != SU do
      putChar(ch)
      nextChar()

    if ch == ']' then
      putChar(ch)
      nextChar()

      if ch == '(' then
        putChar(ch)
        nextChar()

        // Read URL
        while ch != ')' && ch != LF && ch != CR && ch != SU do
          putChar(ch)
          nextChar()

        if ch == ')' then
          putChar(ch)
          nextChar()

    current.token =
      if isImage then
        MarkdownToken.IMAGE
      else
        MarkdownToken.LINK
    current.str = flushTokenString()

  end scanLinkOrImage

  /**
    * Scan plain text until special character
    */
  private def scanText(): Unit =
    while ch != LF && ch != CR && ch != SU && !isSpecialChar(ch) do
      putChar(ch)
      nextChar()

    current.token = MarkdownToken.TEXT
    current.str = flushTokenString()

  /**
    * Check if character is a Markdown special character
    */
  private def isSpecialChar(c: Char): Boolean =
    c match
      case '#' | '`' | '*' | '_' | '[' | ']' | '(' | ')' | '!' | '>' | '-' | '+' =>
        true
      case _ =>
        false

  /**
    * Scan whitespace tokens and handle newlines
    */
  private def scanWhiteSpaces(): Unit =
    while ch == ' ' || ch == '\t' || ch == CR || ch == LF || ch == FF do
      if ch == LF || ch == CR then
        putChar('\n')
        nextChar()
        current.token = MarkdownToken.NEWLINE
        current.str = flushTokenString()
        return
      else
        putChar(ch)
        nextChar()

    current.token = MarkdownToken.WHITESPACE
    current.str = flushTokenString()

end MarkdownScanner
