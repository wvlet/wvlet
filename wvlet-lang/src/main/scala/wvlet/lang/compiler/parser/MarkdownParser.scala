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
          scanner.nextToken() // Skip unexpected tokens

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

    // For simplicity, treat blockquote content as a paragraph
    // In a full implementation, this would recursively parse nested blocks
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
      ordered = false,
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
    val inlineContent = parseInlineContent()
    val firstToken    = inlineContent.headOption

    Paragraph(content = inlineContent, nodeLocation = LinePosition.NoPosition, span = Span.NoSpan)

  /**
    * Parse inline content (text, bold, italic, links, etc.)
    */
  private def parseInlineContent(): List[InlineContent] =
    val content = List.newBuilder[InlineContent]

    var done = false
    while !done do
      val token = scanner.lookAhead()
      token.token match
        case MarkdownToken.EOF | MarkdownToken.NEWLINE =>
          done = true
        case MarkdownToken.HEADING | MarkdownToken.FENCE_MARKER | MarkdownToken.BLOCKQUOTE |
            MarkdownToken.LIST_ITEM | MarkdownToken.HORIZONTAL_RULE =>
          // Block-level token, stop parsing inline content
          done = true
        case MarkdownToken.WHITESPACE =>
          scanner.nextToken()
          // Skip whitespace or accumulate it
        case MarkdownToken.BOLD =>
          content += parseBold()
        case MarkdownToken.ITALIC =>
          content += parseItalic()
        case MarkdownToken.CODE_SPAN =>
          content += parseCodeSpan()
        case MarkdownToken.LINK =>
          content += parseLink()
        case MarkdownToken.IMAGE =>
          content += parseImage()
        case MarkdownToken.TEXT =>
          content += parseText()
        case _ =>
          // Unknown inline token, treat as text
          val t = scanner.nextToken()
          content += Text(t.str, t.nodeLocation, t.span)

    content.result()

  end parseInlineContent

  /**
    * Parse bold text: **text** or __text__
    */
  private def parseBold(): Bold =
    val token = scanner.nextToken()
    Bold(text = token.str, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse italic text: *text* or _text_
    */
  private def parseItalic(): Italic =
    val token = scanner.nextToken()
    Italic(text = token.str, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse inline code span: `code`
    */
  private def parseCodeSpan(): CodeSpan =
    val token = scanner.nextToken()
    CodeSpan(code = token.str, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse link: [text](url)
    */
  private def parseLink(): Link =
    val token       = scanner.nextToken()
    val (text, url) = parseLinkComponents(token.str)

    Link(text = text, url = url, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse image: ![alt](url)
    */
  private def parseImage(): Image =
    val token          = scanner.nextToken()
    val (altText, url) = parseLinkComponents(token.str.drop(1)) // Remove leading !

    Image(altText = altText, url = url, nodeLocation = token.nodeLocation, span = token.span)

  /**
    * Parse plain text
    */
  private def parseText(): Text =
    val token = scanner.nextToken()
    Text(text = token.str, nodeLocation = token.nodeLocation, span = token.span)

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
