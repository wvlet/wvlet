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

import wvlet.lang.api.Span
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.parser.ScannerConfig
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

/**
  * Parser for Markdown files with blank line attachment support
  */
class MarkdownParser(unit: CompilationUnit) extends LogSupport:
  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = MarkdownScanner(
    unit.sourceFile,
    ScannerConfig(skipWhiteSpace = false)
  )

  private var lastToken: TokenData[MarkdownToken] = null

  def parse(): MarkdownDocument =
    val blocks        = List.newBuilder[MarkdownBlock]
    val startTokenOpt = Option(scanner.lookAhead())

    while scanner.lookAhead().token != MarkdownToken.EOF do
      val block = parseBlock()
      if block != null then
        blocks += block

    val (docSpan, docRaw) =
      startTokenOpt match
        case Some(startToken) =>
          val span = spanFrom(startToken)
          span -> textOf(span)
        case None =>
          Span.NoSpan -> ""

    val doc = MarkdownDocument(blocks.result(), docSpan, docRaw)

    // Attach blank lines to blocks (similar to comment attachment in WvletParser)
    attachBlankLines(doc)

  /**
    * Attach blank line tokens to appropriate blocks
    */
  private def attachBlankLines(doc: MarkdownDocument): MarkdownDocument =
    val allNodes = doc
      .collectAllNodes
      .filter(_.span.nonEmpty)
      .sortBy(x => (x.span.start, -x.span.end))
    val blankLines = scanner.getBlankLineTokens().sortBy(_.span.end).reverse

    // Similar logic to WvletParser.attachComments
    // For simplicity in initial implementation, attach blank lines as comments
    blankLines.foreach { blankLine =>
      // Find the node that this blank line should attach to
      allNodes.find(_.span.contains(blankLine.span.start)) match
        case Some(node) =>
          node.withComment(blankLine)
        case None =>
          // Attach to document if no specific node found
          doc.withComment(blankLine)
    }

    doc

  private def parseBlock(): MarkdownBlock =
    val lookahead = scanner.lookAhead()

    lookahead.token match
      case MarkdownToken.HEADING =>
        parseHeading()
      case MarkdownToken.FENCE =>
        parseCodeBlock()
      case MarkdownToken.BLOCKQUOTE =>
        parseBlockquote()
      case MarkdownToken.LIST_MARKER =>
        parseListItem()
      case MarkdownToken.HR =>
        val t = scanner.nextToken()
        lastToken = t
        MarkdownHorizontalRule(t.span, textOf(t.span))
      case MarkdownToken.TEXT | MarkdownToken.WHITESPACE =>
        parseParagraph()
      case MarkdownToken.NEWLINE =>
        // Skip standalone newlines between blocks
        scanner.nextToken()
        null
      case MarkdownToken.EOF =>
        null
      case _ =>
        // Unknown token, treat as text
        val t = scanner.nextToken()
        lastToken = t
        MarkdownText(t.span, textOf(t.span))

  private def parseHeading(): MarkdownHeading =
    val startToken = scanner.nextToken()
    lastToken = startToken

    // Count # characters to determine level
    val text  = startToken.str
    var level = 0
    var i     = 0
    while i < text.length && text.charAt(i) == '#' && level < 6 do
      level += 1
      i += 1

    MarkdownHeading(level, startToken.span, textOf(startToken.span))

  private def parseCodeBlock(): MarkdownCodeBlock =
    val fenceToken = scanner.nextToken()
    lastToken = fenceToken

    // Extract language hint from fence token
    val fenceText = fenceToken.str
    val language =
      if fenceText.length > 3 then
        Some(fenceText.substring(3).trim).filter(_.nonEmpty)
      else
        None

    val startSpan = fenceToken.span

    // Read until closing fence
    var inCode = true
    while inCode && scanner.lookAhead().token != MarkdownToken.EOF do
      val t = scanner.nextToken()
      lastToken = t
      t.token match
        case MarkdownToken.FENCE =>
          // Found closing fence
          inCode = false
        case _ =>
        // Skip content tokens - text will be extracted from span

    val endSpan =
      if lastToken != null then
        lastToken.span
      else
        startSpan
    val blockSpan = startSpan.extendTo(endSpan)
    MarkdownCodeBlock(language, blockSpan, textOf(blockSpan))

  end parseCodeBlock

  private def parseBlockquote(): MarkdownBlockquote =
    val t = scanner.nextToken()
    lastToken = t
    MarkdownBlockquote(t.span, textOf(t.span))

  private def parseListItem(): MarkdownListItem =
    val t = scanner.nextToken()
    lastToken = t
    MarkdownListItem(t.span, textOf(t.span))

  private def parseParagraph(): MarkdownParagraph | Null =
    val startToken = scanner.nextToken()
    lastToken = startToken
    var currentSpan = startToken.span
    var hasText     = startToken.token == MarkdownToken.TEXT

    // Read consecutive text tokens until blank line or block marker
    var continue = true
    while continue do
      val lookahead = scanner.lookAhead()
      lookahead.token match
        case MarkdownToken.TEXT | MarkdownToken.WHITESPACE =>
          val t = scanner.nextToken()
          lastToken = t
          currentSpan = currentSpan.extendTo(t.span)
          if t.token == MarkdownToken.TEXT then
            hasText = true
        case MarkdownToken.NEWLINE =>
          // Consume the newline to check if it's a blank line
          val newlineToken = scanner.nextToken()
          lastToken = newlineToken
          if isBlankLineAfter(newlineToken) then
            continue = false
          else
            // Single newline within paragraph (soft line break)
            currentSpan = currentSpan.extendTo(newlineToken.span)
        case MarkdownToken.EOF =>
          continue = false
        case _ =>
          continue = false

    if hasText then
      MarkdownParagraph(currentSpan, textOf(currentSpan))
    else
      null

  private def spanFrom(startToken: TokenData[MarkdownToken]): Span =
    if lastToken != null then
      startToken.span.extendTo(lastToken.span)
    else
      startToken.span

  private def textOf(span: Span): String =
    if span == Span.NoSpan || span.start < 0 || span.end <= span.start then
      ""
    else
      val buf   = src.getContent
      val start = math.max(0, math.min(span.start, buf.length))
      val end   = math.max(start, math.min(span.end, buf.length))
      buf.slice(start, end).mkString

  private def isBlankLineAfter(newlineToken: TokenData[MarkdownToken]): Boolean =
    val content = src.getContent
    val length  = content.length
    var idx     = newlineToken.span.end

    while idx < length do
      content(idx) match
        case ' ' | '\t' =>
          idx += 1
        case '\r' | '\n' =>
          return true
        case _ =>
          return false

    false

end MarkdownParser
