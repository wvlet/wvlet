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

import TokenType.*

/**
  * Token definitions for Markdown parsing following CommonMark specification. Supports block-level
  * elements (headings, code blocks, lists) and inline elements (bold, italic, links).
  */
enum MarkdownToken(val tokenType: TokenType, val str: String):

  // Control tokens
  case EMPTY      extends MarkdownToken(Control, "<empty>")
  case ERROR      extends MarkdownToken(Control, "<erroneous token>")
  case EOF        extends MarkdownToken(Control, "<eof>")
  case NEWLINE    extends MarkdownToken(Control, "<newline>")
  case WHITESPACE extends MarkdownToken(Control, "<whitespace>")

  // Block-level tokens
  case HEADING         extends MarkdownToken(Control, "<heading>")    // # Heading
  case CODE_BLOCK      extends MarkdownToken(Literal, "<code block>") // Fenced code block
  case BLOCKQUOTE      extends MarkdownToken(Control, "<blockquote>") // > Quote
  case LIST_ITEM       extends MarkdownToken(Control, "<list item>")  // - or * or + or 1.
  case HORIZONTAL_RULE extends MarkdownToken(Control, "<hr>")         // --- or *** or ___
  case PARAGRAPH       extends MarkdownToken(Control, "<paragraph>")  // Plain text block

  // Inline tokens
  case BOLD      extends MarkdownToken(Literal, "<bold>")      // **text** or __text__
  case ITALIC    extends MarkdownToken(Literal, "<italic>")    // *text* or _text_
  case CODE_SPAN extends MarkdownToken(Literal, "<code span>") // `code`
  case LINK      extends MarkdownToken(Literal, "<link>")      // [text](url)
  case IMAGE     extends MarkdownToken(Literal, "<image>")     // ![alt](url)
  case TEXT      extends MarkdownToken(Literal, "<text>")      // Plain text content

  // Special markers
  case FENCE_MARKER extends MarkdownToken(Op, "```") // Code fence marker
  case HASH         extends MarkdownToken(Op, "#")   // Heading marker
  case BULLET       extends MarkdownToken(Op, "-")   // List bullet
  case ASTERISK     extends MarkdownToken(Op, "*")   // Emphasis or bullet
  case UNDERSCORE   extends MarkdownToken(Op, "_")   // Emphasis marker
  case PLUS         extends MarkdownToken(Op, "+")   // List bullet
  case GT           extends MarkdownToken(Op, ">")   // Blockquote marker
  case L_BRACKET    extends MarkdownToken(Op, "[")   // Link/image start
  case R_BRACKET    extends MarkdownToken(Op, "]")   // Link/image middle
  case L_PAREN      extends MarkdownToken(Op, "(")   // URL start
  case R_PAREN      extends MarkdownToken(Op, ")")   // URL end
  case BACKTICK     extends MarkdownToken(Op, "`")   // Code span marker
  case EXCLAMATION  extends MarkdownToken(Op, "!")   // Image prefix

  def isBlockToken: Boolean =
    this match
      case HEADING | CODE_BLOCK | BLOCKQUOTE | LIST_ITEM | HORIZONTAL_RULE | PARAGRAPH =>
        true
      case _ =>
        false

  def isInlineToken: Boolean =
    this match
      case BOLD | ITALIC | CODE_SPAN | LINK | IMAGE | TEXT =>
        true
      case _ =>
        false

  def isMarker: Boolean = tokenType == Op

end MarkdownToken

object MarkdownToken:

  val blockTokens = Set(
    MarkdownToken.HEADING,
    MarkdownToken.CODE_BLOCK,
    MarkdownToken.BLOCKQUOTE,
    MarkdownToken.LIST_ITEM,
    MarkdownToken.HORIZONTAL_RULE,
    MarkdownToken.PARAGRAPH
  )

  val inlineTokens = Set(
    MarkdownToken.BOLD,
    MarkdownToken.ITALIC,
    MarkdownToken.CODE_SPAN,
    MarkdownToken.LINK,
    MarkdownToken.IMAGE,
    MarkdownToken.TEXT
  )

  val emphasisMarkers = Set(MarkdownToken.ASTERISK, MarkdownToken.UNDERSCORE)

  val listBullets = Set(MarkdownToken.BULLET, MarkdownToken.ASTERISK, MarkdownToken.PLUS)

  /**
    * TokenTypeInfo implementation for MarkdownToken Provides metadata and lookup capabilities for
    * the scanner
    */
  given tokenTypeInfo: TokenTypeInfo[MarkdownToken] with
    override def empty: MarkdownToken      = MarkdownToken.EMPTY
    override def errorToken: MarkdownToken = MarkdownToken.ERROR
    override def eofToken: MarkdownToken   = MarkdownToken.EOF
    override def identifier: MarkdownToken = MarkdownToken.TEXT
    override def findToken(s: String): Option[MarkdownToken] =
      s match
        case "#" =>
          Some(MarkdownToken.HASH)
        case "-" =>
          Some(MarkdownToken.BULLET)
        case "*" =>
          Some(MarkdownToken.ASTERISK)
        case "_" =>
          Some(MarkdownToken.UNDERSCORE)
        case "+" =>
          Some(MarkdownToken.PLUS)
        case ">" =>
          Some(MarkdownToken.GT)
        case "[" =>
          Some(MarkdownToken.L_BRACKET)
        case "]" =>
          Some(MarkdownToken.R_BRACKET)
        case "(" =>
          Some(MarkdownToken.L_PAREN)
        case ")" =>
          Some(MarkdownToken.R_PAREN)
        case "`" =>
          Some(MarkdownToken.BACKTICK)
        case "!" =>
          Some(MarkdownToken.EXCLAMATION)
        case "```" =>
          Some(MarkdownToken.FENCE_MARKER)
        case _ =>
          None

    override def integerLiteral: MarkdownToken       = MarkdownToken.TEXT
    override def longLiteral: MarkdownToken          = MarkdownToken.TEXT
    override def decimalLiteral: MarkdownToken       = MarkdownToken.TEXT
    override def expLiteral: MarkdownToken           = MarkdownToken.TEXT
    override def doubleLiteral: MarkdownToken        = MarkdownToken.TEXT
    override def floatLiteral: MarkdownToken         = MarkdownToken.TEXT
    override def commentToken: MarkdownToken         = MarkdownToken.TEXT
    override def docCommentToken: MarkdownToken      = MarkdownToken.TEXT
    override def singleQuoteString: MarkdownToken    = MarkdownToken.TEXT
    override def doubleQuoteString: MarkdownToken    = MarkdownToken.TEXT
    override def tripleQuoteString: MarkdownToken    = MarkdownToken.TEXT
    override def whiteSpace: MarkdownToken           = MarkdownToken.WHITESPACE
    override def backQuotedIdentifier: MarkdownToken = MarkdownToken.CODE_SPAN

  end tokenTypeInfo

end MarkdownToken
