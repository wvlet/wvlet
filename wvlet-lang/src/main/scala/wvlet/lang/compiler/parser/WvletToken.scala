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

enum TokenType:
  case Control,
    Doc,
    Literal,
    Identifier,
    Quote,
    Op,
    Keyword

import TokenType.*

import scala.annotation.switch

enum WvletToken(val tokenType: TokenType, val str: String):
  def isIdentifier: Boolean          = tokenType == Identifier
  def isLiteral: Boolean             = tokenType == Literal
  def isReservedKeyword: Boolean     = tokenType == Keyword
  def isOperator: Boolean            = tokenType == Op
  def isRightParenOrBracket: Boolean = this == WvletToken.R_PAREN || this == WvletToken.R_BRACKET

  // special tokens
  case EMPTY      extends WvletToken(Control, "<empty>")
  case ERROR      extends WvletToken(Control, "<erroneous token>")
  case EOF        extends WvletToken(Control, "<eof>")
  case NEWLINE    extends WvletToken(Control, "<newline>")
  case WHITESPACE extends WvletToken(Control, "<whitespace>")

  // doc or comments
  case COMMENT extends WvletToken(Doc, "<comment>")

  // For indentation
  case INDENT  extends WvletToken(Control, "<indent>")
  case OUTDENT extends WvletToken(Control, "<outdent>")

  // Literals
  case INTEGER_LITERAL extends WvletToken(Literal, "<integer literal>")
  case DECIMAL_LITERAL extends WvletToken(Literal, "<decimal literal>")
  case EXP_LITERAL     extends WvletToken(Literal, "<exp literal>")
  case LONG_LITERAL    extends WvletToken(Literal, "<long literal>")
  case FLOAT_LITERAL   extends WvletToken(Literal, "<float literal>")
  case DOUBLE_LITERAL  extends WvletToken(Literal, "<double literal>")
  case STRING_LITERAL  extends WvletToken(Literal, "<string literal>")

  // For interpolated string, e.g., sql"...${expr}..."
  case STRING_INTERPOLATION_PREFIX extends WvletToken(Literal, "<string interpolation>")
  // A part in the string interpolation
  case STRING_PART extends WvletToken(Literal, "<string part>")

  // Identifiers
  case IDENTIFIER extends WvletToken(Identifier, "<identifier>")
  // Identifier wrapped in backquotes `....`
  case BACKQUOTED_IDENTIFIER extends WvletToken(Identifier, "<quoted identifier>")

  case SINGLE_QUOTE extends WvletToken(Quote, "'")
  case DOUBLE_QUOTE extends WvletToken(Quote, "\"")
  case BACK_QUOTE   extends WvletToken(Quote, "`")

  // Parentheses
  case L_PAREN   extends WvletToken(Op, "(")
  case R_PAREN   extends WvletToken(Op, ")")
  case L_BRACE   extends WvletToken(Op, "{")
  case R_BRACE   extends WvletToken(Op, "}")
  case L_BRACKET extends WvletToken(Op, "[")
  case R_BRACKET extends WvletToken(Op, "]")

  // Special symbols
  case COLON      extends WvletToken(Op, ":")
  case COMMA      extends WvletToken(Op, ",")
  case DOT        extends WvletToken(Op, ".")
  case UNDERSCORE extends WvletToken(Op, "_")
  case AT         extends WvletToken(Op, "@")
  case DOLLAR     extends WvletToken(Op, "$")
  case STAR       extends WvletToken(Op, "*")
  case QUESTION   extends WvletToken(Op, "?")

  case L_ARROW        extends WvletToken(Op, "<-")
  case R_ARROW        extends WvletToken(Op, "->")
  case R_DOUBLE_ARROW extends WvletToken(Op, "=>")

  // Special keywords
  case EQ   extends WvletToken(Op, "=")
  case NEQ  extends WvletToken(Op, "!=")
  case LT   extends WvletToken(Op, "<")
  case GT   extends WvletToken(Op, ">")
  case LTEQ extends WvletToken(Op, "<=")
  case GTEQ extends WvletToken(Op, ">=")

  case PLUS    extends WvletToken(Op, "+")
  case MINUS   extends WvletToken(Op, "-")
  case DIV     extends WvletToken(Op, "/")
  case DIV_INT extends WvletToken(Op, "//")
  case MOD     extends WvletToken(Op, "%")

  case EXCLAMATION extends WvletToken(Op, "!")

  case AMP  extends WvletToken(Op, "&")
  case PIPE extends WvletToken(Op, "|")

  case HASH extends WvletToken(Op, "#")

  // literal keywords
  case NULL  extends WvletToken(Keyword, "null")
  case TRUE  extends WvletToken(Keyword, "true")
  case FALSE extends WvletToken(Keyword, "false")

  // For testing
  case SHOULD  extends WvletToken(Keyword, "should")
  case BE      extends WvletToken(Keyword, "be")
  case CONTAIN extends WvletToken(Keyword, "contain")

  // Alphabectic keywords
  case DEF     extends WvletToken(Keyword, "def")
  case INLINE  extends WvletToken(Keyword, "inline")
  case SCHEMA  extends WvletToken(Keyword, "schema")
  case TYPE    extends WvletToken(Keyword, "type")
  case EXTENDS extends WvletToken(Keyword, "extends")
  case WITH    extends WvletToken(Keyword, "with")
  case TEST    extends WvletToken(Keyword, "test")
  case SHOW    extends WvletToken(Keyword, "show")
  case SAMPLE  extends WvletToken(Keyword, "sample")

  case THIS extends WvletToken(Keyword, "this")

  case OF extends WvletToken(Keyword, "of")
  case IN extends WvletToken(Keyword, "in")
  case BY extends WvletToken(Keyword, "by")
  case AS extends WvletToken(Keyword, "as")

  case FROM      extends WvletToken(Keyword, "from")
  case AGG       extends WvletToken(Keyword, "agg")
  case SELECT    extends WvletToken(Keyword, "select")
  case FOR       extends WvletToken(Keyword, "for")
  case LET       extends WvletToken(Keyword, "let")
  case WHERE     extends WvletToken(Keyword, "where")
  case GROUP     extends WvletToken(Keyword, "group")
  case HAVING    extends WvletToken(Keyword, "having")
  case ORDER     extends WvletToken(Keyword, "order")
  case LIMIT     extends WvletToken(Keyword, "limit")
  case TRANSFORM extends WvletToken(Keyword, "transform")
  case PIVOT     extends WvletToken(Keyword, "pivot")

  case DISTINCT extends WvletToken(Keyword, "distinct")

  // for order by
  case ASC  extends WvletToken(Keyword, "asc")
  case DESC extends WvletToken(Keyword, "desc")

  // join keywords
  case JOIN  extends WvletToken(Keyword, "join")
  case ON    extends WvletToken(Keyword, "on")
  case LEFT  extends WvletToken(Keyword, "left")
  case RIGHT extends WvletToken(Keyword, "right")
  case FULL  extends WvletToken(Keyword, "full")
  case INNER extends WvletToken(Keyword, "inner")
  case CROSS extends WvletToken(Keyword, "cross")

  // ddl keywords
  case ADD      extends WvletToken(Keyword, "add")
  case EXCLUDE  extends WvletToken(Keyword, "exclude")
  case SHIFT    extends WvletToken(Keyword, "shift")
  case TO       extends WvletToken(Keyword, "to")
  case DROP     extends WvletToken(Keyword, "drop")
  case DESCRIBE extends WvletToken(Keyword, "describe")

  // window function keywords
  case OVER      extends WvletToken(Keyword, "over")
  case PARTITION extends WvletToken(Keyword, "partition")
  case UNBOUNDED extends WvletToken(Keyword, "unbounded")
  case PRECEDING extends WvletToken(Keyword, "preceding")
  case FOLLOWING extends WvletToken(Keyword, "following")
  case CURRENT   extends WvletToken(Keyword, "current")
  case RANGE     extends WvletToken(Keyword, "range")
  case ROW       extends WvletToken(Keyword, "row")

  case RUN     extends WvletToken(Keyword, "run")
  case IMPORT  extends WvletToken(Keyword, "import")
  case EXPORT  extends WvletToken(Keyword, "export")
  case PACKAGE extends WvletToken(Keyword, "package")
  case MODEL   extends WvletToken(Keyword, "model")

  case IF   extends WvletToken(Keyword, "if")
  case THEN extends WvletToken(Keyword, "then")
  case ELSE extends WvletToken(Keyword, "else")
  case END  extends WvletToken(Keyword, "end")

  case AND  extends WvletToken(Keyword, "and")
  case OR   extends WvletToken(Keyword, "or")
  case NOT  extends WvletToken(Keyword, "not")
  case IS   extends WvletToken(Keyword, "is")
  case LIKE extends WvletToken(Keyword, "like")

end WvletToken

object WvletToken:
  val keywords       = WvletToken.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = WvletToken.values.filter(_.tokenType == Op).toSeq

  val allKeywordAndSymbol = keywords ++ specialSymbols

  val keywordAndSymbolTable = allKeywordAndSymbol.map(x => x.str -> x).toMap

  val joinKeywords = List(
    WvletToken.JOIN,
    WvletToken.ON,
    WvletToken.LEFT,
    WvletToken.RIGHT,
    WvletToken.FULL,
    WvletToken.INNER,
    WvletToken.CROSS
  )

  val queryBlockKeywords =
    List(
      WvletToken.FROM,
      WvletToken.SELECT,
      WvletToken.FOR,
      WvletToken.LET,
      WvletToken.WHERE,
      WvletToken.GROUP,
      WvletToken.HAVING,
      WvletToken.ORDER,
      WvletToken.LIMIT,
      WvletToken.TRANSFORM,
      WvletToken.TEST
    ) ++ joinKeywords

  val queryEndKeywords = Set(WvletToken.EOF, WvletToken.END, WvletToken.R_PAREN)

  def isQueryEndKeyword(t: WvletToken): Boolean = queryEndKeywords.contains(t)

  // Line Feed '\n'
  inline val LF = '\u000A'
  // Form Feed '\f'
  inline val FF = '\u000C'
  // Carriage Return '\r'
  inline val CR = '\u000D'
  // Substitute (SUB), which is used as the EOF marker in Windows
  inline val SU = '\u001A'

  def isLineBreakChar(c: Char): Boolean =
    (c: @switch) match
      case LF | FF | CR | SU =>
        true
      case _ =>
        false

  /**
    * White space character but not a new line (\n)
    *
    * @param c
    * @return
    */
  def isWhiteSpaceChar(c: Char): Boolean =
    (c: @switch) match
      case ' ' | '\t' | CR =>
        true
      case _ =>
        false

  def isNumberSeparator(ch: Char): Boolean = ch == '_'

  /**
    * Convert a character to an integer value using the given base. Returns -1 upon failures
    */
  def digit2int(ch: Char, base: Int): Int =
    val num =
      if ch <= '9' then
        ch - '0'
      else if 'a' <= ch && ch <= 'z' then
        ch - 'a' + 10
      else if 'A' <= ch && ch <= 'Z' then
        ch - 'A' + 10
      else
        -1
    if 0 <= num && num < base then
      num
    else
      -1

end WvletToken
