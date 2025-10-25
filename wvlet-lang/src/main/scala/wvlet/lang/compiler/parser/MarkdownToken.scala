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
  * Token types for Markdown syntax
  */
enum MarkdownToken(val tokenType: TokenType, val str: String):
  // Control tokens
  case EMPTY      extends MarkdownToken(Control, "<empty>")
  case ERROR      extends MarkdownToken(Control, "<error>")
  case EOF        extends MarkdownToken(Control, "<eof>")
  case NEWLINE    extends MarkdownToken(Control, "<newline>")
  case BLANK_LINE extends MarkdownToken(Control, "<blank-line>")

  // Block markers
  case HEADING     extends MarkdownToken(Keyword, "<heading>")
  case FENCE       extends MarkdownToken(Keyword, "```")
  case BLOCKQUOTE  extends MarkdownToken(Keyword, ">")
  case LIST_MARKER extends MarkdownToken(Keyword, "<list-marker>")
  case HR          extends MarkdownToken(Keyword, "---")

  // Inline markers (for future inline parsing)
  case BOLD        extends MarkdownToken(Keyword, "**")
  case ITALIC      extends MarkdownToken(Keyword, "*")
  case CODE_SPAN   extends MarkdownToken(Keyword, "`")
  case LINK_START  extends MarkdownToken(Keyword, "[")
  case IMAGE_START extends MarkdownToken(Keyword, "![")

  // Text content
  case TEXT       extends MarkdownToken(Literal, "<text>")
  case WHITESPACE extends MarkdownToken(Control, "<whitespace>")

/**
  * TokenTypeInfo typeclass implementation for MarkdownToken
  */
given TokenTypeInfo[MarkdownToken] with
  override def empty: MarkdownToken                        = MarkdownToken.EMPTY
  override def errorToken: MarkdownToken                   = MarkdownToken.ERROR
  override def eofToken: MarkdownToken                     = MarkdownToken.EOF
  override def identifier: MarkdownToken                   = MarkdownToken.TEXT
  override def findToken(s: String): Option[MarkdownToken] = None
  override def integerLiteral: MarkdownToken               = MarkdownToken.TEXT
  override def longLiteral: MarkdownToken                  = MarkdownToken.TEXT
  override def decimalLiteral: MarkdownToken               = MarkdownToken.TEXT
  override def expLiteral: MarkdownToken                   = MarkdownToken.TEXT
  override def doubleLiteral: MarkdownToken                = MarkdownToken.TEXT
  override def floatLiteral: MarkdownToken                 = MarkdownToken.TEXT
  override def commentToken: MarkdownToken                 = MarkdownToken.TEXT
  override def docCommentToken: MarkdownToken              = MarkdownToken.TEXT
  override def singleQuoteString: MarkdownToken            = MarkdownToken.TEXT
  override def doubleQuoteString: MarkdownToken            = MarkdownToken.TEXT
  override def tripleQuoteString: MarkdownToken            = MarkdownToken.TEXT
  override def whiteSpace: MarkdownToken                   = MarkdownToken.WHITESPACE
  override def backQuotedIdentifier: MarkdownToken         = MarkdownToken.TEXT
