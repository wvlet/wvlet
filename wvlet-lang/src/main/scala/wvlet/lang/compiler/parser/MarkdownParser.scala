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
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

/**
  * Parser for Markdown files with blank line attachment support
  */
class MarkdownParser(unit: CompilationUnit) extends LogSupport:
  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = MarkdownScanner(unit.sourceFile)

  private var lastToken: TokenData[MarkdownToken] = null

  def parse(): MarkdownDocument =
    val blocks        = List.newBuilder[MarkdownBlock]
    val startTokenOpt = Option(scanner.lookAhead())

    while scanner.lookAhead().token != MarkdownToken.EOF do
      val block = parseBlock()
      if block != null then
        blocks += block

    val doc =
      startTokenOpt match
        case Some(startToken) =>
          MarkdownDocument(blocks.result(), spanFrom(startToken))
        case None =>
          MarkdownDocument(blocks.result(), Span.NoSpan)

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
    val t = scanner.nextToken()
    lastToken = t

    t.token match
      case MarkdownToken.HEADING =>
        parseHeading(t)
      case MarkdownToken.FENCE =>
        parseCodeBlock(t)
      case MarkdownToken.BLOCKQUOTE =>
        parseBlockquote(t)
      case MarkdownToken.LIST_MARKER =>
        parseListItem(t)
      case MarkdownToken.HR =>
        MarkdownHorizontalRule(t.span)
      case MarkdownToken.TEXT =>
        parseParagraph(t)
      case MarkdownToken.NEWLINE | MarkdownToken.WHITESPACE =>
        // Skip newlines and whitespace between blocks
        null
      case MarkdownToken.EOF =>
        null
      case _ =>
        // Unknown token, treat as text
        MarkdownText(t.span)

  private def parseHeading(startToken: TokenData[MarkdownToken]): MarkdownHeading =
    // Count # characters to determine level
    val text  = startToken.str
    var level = 0
    var i     = 0
    while i < text.length && text.charAt(i) == '#' && level < 6 do
      level += 1
      i += 1

    MarkdownHeading(level, startToken.span)

  private def parseCodeBlock(fenceToken: TokenData[MarkdownToken]): MarkdownCodeBlock =
    // Extract language hint from fence token
    val fenceText = fenceToken.str
    val language =
      if fenceText.length > 3 then
        Some(fenceText.substring(3).trim).filter(_.nonEmpty)
      else
        None

    val startSpan = fenceToken.span
    val codeLines = List.newBuilder[String]

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
          codeLines += t.str

    val endSpan =
      if lastToken != null then
        lastToken.span
      else
        startSpan
    MarkdownCodeBlock(language, startSpan.extendTo(endSpan))

  end parseCodeBlock

  private def parseBlockquote(t: TokenData[MarkdownToken]): MarkdownBlockquote = MarkdownBlockquote(
    t.span
  )

  private def parseListItem(t: TokenData[MarkdownToken]): MarkdownListItem = MarkdownListItem(
    t.span
  )

  private def parseParagraph(startToken: TokenData[MarkdownToken]): MarkdownParagraph =
    var currentSpan = startToken.span

    // Read consecutive text tokens until blank line or block marker
    var continue = true
    while continue && scanner.lookAhead().token != MarkdownToken.EOF do
      val lookahead = scanner.lookAhead()
      lookahead.token match
        case MarkdownToken.TEXT | MarkdownToken.WHITESPACE =>
          val t = scanner.nextToken()
          lastToken = t
          currentSpan = currentSpan.extendTo(t.span)
        case MarkdownToken.NEWLINE =>
          // Consume the newline to check if it's a blank line
          val newlineToken = scanner.nextToken()
          lastToken = newlineToken
          val next = scanner.lookAhead()
          if next.token == MarkdownToken.NEWLINE || next.token == MarkdownToken.BLANK_LINE then
            // Double newline = blank line, end paragraph
            continue = false
          else
            // Single newline within paragraph (soft line break)
            currentSpan = currentSpan.extendTo(newlineToken.span)
        case _ =>
          continue = false

    MarkdownParagraph(currentSpan)

  private def spanFrom(startToken: TokenData[MarkdownToken]): Span =
    if lastToken != null then
      startToken.span.extendTo(lastToken.span)
    else
      startToken.span

end MarkdownParser
