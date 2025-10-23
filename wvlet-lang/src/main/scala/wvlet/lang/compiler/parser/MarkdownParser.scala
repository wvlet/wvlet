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

import wvlet.lang.api.{LinePosition, Span}
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.model.plan.*
import wvlet.log.LogSupport

/**
  * Parser for Markdown files following CommonMark specification Uses recursive descent parsing to
  * build MarkdownDocument AST
  */
class MarkdownParser(unit: CompilationUnit) extends LogSupport:

  given src: SourceFile                  = unit.sourceFile
  given compilationUnit: CompilationUnit = unit

  private val scanner = MarkdownScanner(
    unit.sourceFile,
    ScannerConfig(skipComments = false, skipWhiteSpace = false)
  )

  /**
    * Parse the Markdown document
    */
  def parse(): LogicalPlan =
    val blocks = parseBlocks()
    MarkdownDocument(blocks)

  /**
    * Parse all blocks in the document
    */
  private def parseBlocks(): List[MarkdownBlock] =
    val blocks = List.newBuilder[MarkdownBlock]

    var done = false
    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.EOF =>
          done = true
        case MarkdownToken.WHITESPACE | MarkdownToken.NEWLINE =>
          scanner.nextToken() // Skip
        case MarkdownToken.HEADING =>
          blocks += parseHeading()
        case MarkdownToken.FENCE_MARKER =>
          blocks += parseCodeBlock()
        case MarkdownToken.BLOCKQUOTE =>
          blocks += parseBlockquote()
        case MarkdownToken.LIST_ITEM =>
          blocks += parseList()
        case MarkdownToken.HORIZONTAL_RULE =>
          blocks += parseHorizontalRule()
        case _ =>
          blocks += parseParagraph()

    blocks.result()

  /**
    * Parse heading: # through ######
    */
  private def parseHeading(): Heading =
    val token    = scanner.nextToken()
    val fullText = token.str

    // Count leading # characters
    var level = 0
    var i     = 0
    while i < fullText.length && fullText(i) == '#' && level < 6 do
      level += 1
      i += 1

    // Extract text after hash marks and optional whitespace
    val text = fullText.drop(level).trim()

    Heading(level = level, text = text, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse fenced code block: ```language\ncode\n```
    */
  private def parseCodeBlock(): CodeBlock =
    val openToken = scanner.nextToken() // Opening ```
    val openText  = openToken.str

    // Extract language hint from the opening fence line
    val languageHint = extractLanguageHint(openText)

    // Collect code lines
    val codeLines = List.newBuilder[String]
    var done      = false

    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.FENCE_MARKER =>
          scanner.nextToken() // Consume closing fence
          done = true
        case MarkdownToken.CODE_BLOCK =>
          val codeToken = scanner.nextToken()
          codeLines += codeToken.str
        case MarkdownToken.EOF =>
          done = true
        case _ =>
          // Treat unexpected tokens as part of the code content to prevent data loss
          val unexpectedToken = scanner.nextToken()
          codeLines += unexpectedToken.str

    CodeBlock(
      language = languageHint,
      code = codeLines.result().mkString,
      nodeLocation = openToken.nodeLocation,
      span = openToken.span
    )

  end parseCodeBlock

  /**
    * Extract language hint from fence marker line
    */
  private def extractLanguageHint(fenceLine: String): Option[String] =
    // Remove leading backticks and trim
    val withoutFence = fenceLine.dropWhile(_ == '`').trim()
    if withoutFence.isEmpty then
      None
    else
      Some(withoutFence)

  /**
    * Parse blockquote: > quoted text
    */
  private def parseBlockquote(): Blockquote =
    val quoteToken = scanner.nextToken()

    // TODO: For CommonMark compliance, this should recursively parse nested blocks within blockquotes
    // including support for multiple paragraphs, lists, and nested blockquotes.
    // Current implementation is simplified and only handles a single paragraph of content.
    val content = List.newBuilder[MarkdownBlock]

    var done = false
    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.NEWLINE | MarkdownToken.EOF =>
          done = true
        case MarkdownToken.BLOCKQUOTE =>
          scanner.nextToken()
          // Continue reading blockquote
        case _ =>
          content += parseParagraph()
          done = true

    Blockquote(
      content = content.result(),
      nodeLocation = quoteToken.nodeLocation,
      span = quoteToken.span
    )

  /**
    * Parse list (unordered or ordered)
    */
  private def parseList(): ListBlock =
    val items      = List.newBuilder[ListItem]
    val firstToken = scanner.lookAhead()

    // Detect if this is an ordered list by checking the first list item marker
    val isOrdered = firstToken.str.exists(_.isDigit)

    var done = false
    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.LIST_ITEM =>
          items += parseListItem()
        case MarkdownToken.NEWLINE | MarkdownToken.WHITESPACE =>
          scanner.nextToken() // Skip whitespace between items
        case _ =>
          done = true

    ListBlock(
      items = items.result(),
      ordered = isOrdered,
      nodeLocation = firstToken.nodeLocation,
      span = firstToken.span
    )

  /**
    * Parse single list item
    */
  private def parseListItem(): ListItem =
    val bulletToken = scanner.nextToken()

    // Read content until end of line
    val contentTokens = List.newBuilder[String]
    var done          = false

    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.NEWLINE | MarkdownToken.EOF =>
          done = true
        case MarkdownToken.WHITESPACE =>
          scanner.nextToken()
          contentTokens += " "
        case _ =>
          val t = scanner.nextToken()
          contentTokens += t.str

    ListItem(
      content = contentTokens.result().mkString.trim(),
      nodeLocation = bulletToken.nodeLocation,
      span = bulletToken.span
    )

  /**
    * Parse horizontal rule: ---, ***, ___
    */
  private def parseHorizontalRule(): HorizontalRule =
    val token = scanner.nextToken()
    HorizontalRule(nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse paragraph with inline content
    */
  private def parseParagraph(): Paragraph =
    val inlineExpr = parseInlineExpression()
    Paragraph(content = inlineExpr, nodeLocation = LinePosition.NoPosition, span = Span.NoSpan)

  /**
    * Parse inline content for a paragraph by accumulating inline tokens until a terminating
    * newline/EOF or a new block-level token appears. Returns a single MarkdownExpression: either a
    * single element or a MarkdownSequence wrapping multiple inline elements.
    */
  private def parseInlineExpression(): MarkdownExpression =
    val parts = List.newBuilder[MarkdownExpression]
    var done  = false

    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.EOF | MarkdownToken.NEWLINE =>
          done = true
        case MarkdownToken.HEADING | MarkdownToken.FENCE_MARKER | MarkdownToken.BLOCKQUOTE |
            MarkdownToken.LIST_ITEM | MarkdownToken.HORIZONTAL_RULE =>
          // Encountered a block-level token; finish the current paragraph
          done = true
        case MarkdownToken.WHITESPACE =>
          // Collapse any whitespace into a single space within a paragraph
          scanner.nextToken()
          parts += MarkdownText(" ")
        case MarkdownToken.BOLD =>
          parts += parseBold()
        case MarkdownToken.ITALIC =>
          parts += parseItalic()
        case MarkdownToken.CODE_SPAN =>
          parts += parseCodeSpan()
        case MarkdownToken.LINK =>
          parts += parseLink()
        case MarkdownToken.IMAGE =>
          parts += parseImage()
        case MarkdownToken.TEXT =>
          parts += parseText()
        case _ =>
          // Unknown inline token, consume and continue to avoid infinite loop
          scanner.nextToken()

    // Normalize: drop empty text nodes introduced by whitespace handling
    val normalized = parts
      .result()
      .filter {
        case MarkdownText(t, _, _) =>
          t.nonEmpty
        case _ =>
          true
      }

    normalized match
      case Nil =>
        MarkdownText("", LinePosition.NoPosition, Span.NoSpan)
      case single :: Nil =>
        single
      case many =>
        MarkdownSequence(many, LinePosition.NoPosition, Span.NoSpan)

  end parseInlineExpression

  /**
    * Parse bold text: **text** or __text__
    */
  private def parseBold(): MarkdownBold =
    val token = scanner.nextToken()
    // For now, treat content as simple text (TODO: parse nested inline elements)
    val text = token.str
    MarkdownBold(content = MarkdownText(text), nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse italic text: *text* or _text_
    */
  private def parseItalic(): MarkdownItalic =
    val token = scanner.nextToken()
    // For now, treat content as simple text (TODO: parse nested inline elements)
    val text = token.str
    MarkdownItalic(
      content = MarkdownText(text),
      nodeLocation = token.nodeLocation,
      span = token.span
    )

  /**
    * Parse inline code span: `code`
    */
  private def parseCodeSpan(): MarkdownCodeSpan =
    val token = scanner.nextToken()
    MarkdownCodeSpan(code = token.str, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse link: [text](url)
    */
  private def parseLink(): MarkdownLink =
    val token       = scanner.nextToken()
    val (text, url) = parseLinkComponents(token.str)

    MarkdownLink(
      text = MarkdownText(text),
      url = url,
      nodeLocation = token.nodeLocation,
      span = token.span
    )

  /**
    * Parse image: ![alt](url)
    */
  private def parseImage(): MarkdownImage =
    val token          = scanner.nextToken()
    val (altText, url) = parseLinkComponents(token.str.drop(1)) // Remove leading !

    MarkdownImage(
      altText = altText,
      url = url,
      nodeLocation = token.nodeLocation,
      span = token.span
    )

  /**
    * Parse plain text
    */
  private def parseText(): MarkdownText =
    val token = scanner.nextToken()
    MarkdownText(text = token.str, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse link components from [text](url) format
    */
  private def parseLinkComponents(linkStr: String): (String, String) =
    val textStart = linkStr.indexOf('[')
    val textEnd   = linkStr.indexOf(']')
    val urlStart  = linkStr.indexOf('(')
    val urlEnd    = linkStr.lastIndexOf(')')

    if textStart >= 0 && textEnd > textStart && urlStart >= 0 && urlEnd > urlStart then
      val text = linkStr.substring(textStart + 1, textEnd)
      val url  = linkStr.substring(urlStart + 1, urlEnd)
      (text, url)
    else
      (linkStr, "")

end MarkdownParser
