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
  def isIdentifier: Boolean      = tokenType == Identifier
  def isLiteral: Boolean         = tokenType == Literal
  def isReservedKeyword: Boolean = tokenType == Keyword
  def isNonReservedKeyword: Boolean =
    tokenType == Keyword && WvletToken.nonReservedKeywords.contains(this)

  def canStartSelectItem: Boolean =
    tokenType != Keyword || WvletToken.literalStartKeywords.contains(this) ||
      WvletToken.nonReservedKeywords.contains(this)

  def isOperator: Boolean            = tokenType == Op
  def isRightParenOrBracket: Boolean = this == WvletToken.R_PAREN || this == WvletToken.R_BRACKET

  def isQueryDelimiter: Boolean           = WvletToken.isQueryDelimiter(this)
  def isStringStart: Boolean              = WvletToken.stringStartToken.contains(this)
  def isStringLiteral: Boolean            = WvletToken.stringLiterals.contains(this)
  def isInterpolatedStringPrefix: Boolean = WvletToken.stringInterpolationPrefixes.contains(this)

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
  case INTEGER_LITERAL     extends WvletToken(Literal, "<integer literal>")
  case DECIMAL_LITERAL     extends WvletToken(Literal, "<decimal literal>")
  case EXP_LITERAL         extends WvletToken(Literal, "<exp literal>")
  case LONG_LITERAL        extends WvletToken(Literal, "<long literal>")
  case FLOAT_LITERAL       extends WvletToken(Literal, "<float literal>")
  case DOUBLE_LITERAL      extends WvletToken(Literal, "<double literal>")
  case SINGLE_QUOTE_STRING extends WvletToken(Literal, "<'string literal'>") // Single-quoted
  case DOUBLE_QUOTE_STRING extends WvletToken(Literal, "<\"string literal\">")
  case TRIPLE_QUOTE_STRING extends WvletToken(Literal, "<\"\"\"string literal\"\"\">")

  // literal keywords
  case NULL  extends WvletToken(Keyword, "null")
  case TRUE  extends WvletToken(Keyword, "true")
  case FALSE extends WvletToken(Keyword, "false")

  // For interpolated string, e.g., sql"...${expr}..."
  case STRING_INTERPOLATION_PREFIX extends WvletToken(Literal, "<string interpolation>")
  case TRIPLE_QUOTE_INTERPOLATION_PREFIX
      extends WvletToken(Literal, "<\"\"\"string interpolation\"\"\">")

  // For backquoted interpolation strings, e.g., s`table_name_${expr}...`
  case BACKQUOTE_INTERPOLATION_PREFIX extends WvletToken(Literal, "<backquote interpolation>")
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
  case SEMICOLON  extends WvletToken(Op, ";")
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

  case EXISTS extends WvletToken(Keyword, "exists")

  // For testing
  case TEST    extends WvletToken(Keyword, "test")
  case SHOULD  extends WvletToken(Keyword, "should")
  case BE      extends WvletToken(Keyword, "be")
  case CONTAIN extends WvletToken(Keyword, "contain")
  case DEBUG   extends WvletToken(Keyword, "debug")

  // Type definition keywords
  case DEF     extends WvletToken(Keyword, "def")
  case INLINE  extends WvletToken(Keyword, "inline")
  case TYPE    extends WvletToken(Keyword, "type")
  case EXTENDS extends WvletToken(Keyword, "extends")
  case NATIVE  extends WvletToken(Keyword, "native")
  case THIS    extends WvletToken(Keyword, "this")

  // Data type keywords
  case MAP extends WvletToken(Keyword, "map")

  case SHOW   extends WvletToken(Keyword, "show")
  case SAMPLE extends WvletToken(Keyword, "sample")

  // Operator prepositions
  case OF   extends WvletToken(Keyword, "of")
  case IN   extends WvletToken(Keyword, "in")
  case BY   extends WvletToken(Keyword, "by")
  case AS   extends WvletToken(Keyword, "as")
  case TO   extends WvletToken(Keyword, "to")
  case WITH extends WvletToken(Keyword, "with")

  case FROM   extends WvletToken(Keyword, "from")
  case AGG    extends WvletToken(Keyword, "agg")
  case SELECT extends WvletToken(Keyword, "select")
  case FOR    extends WvletToken(Keyword, "for")
  case LET    extends WvletToken(Keyword, "let")
  case WHERE  extends WvletToken(Keyword, "where")
  case GROUP  extends WvletToken(Keyword, "group")
  case HAVING extends WvletToken(Keyword, "having")
  case ORDER  extends WvletToken(Keyword, "order")
  case LIMIT  extends WvletToken(Keyword, "limit")
  // case TRANSFORM extends WvletToken(Keyword, "transform")
  case PIVOT extends WvletToken(Keyword, "pivot")

  case COUNT    extends WvletToken(Keyword, "count")
  case DISTINCT extends WvletToken(Keyword, "distinct")

  // for order by
  case ASC   extends WvletToken(Keyword, "asc")
  case DESC  extends WvletToken(Keyword, "desc")
  case NULLS extends WvletToken(Keyword, "nulls")
  case FIRST extends WvletToken(Keyword, "first")
  case LAST  extends WvletToken(Keyword, "last")

  // join keywords
  case JOIN  extends WvletToken(Keyword, "join")
  case ASOF  extends WvletToken(Keyword, "asof")
  case ON    extends WvletToken(Keyword, "on")
  case LEFT  extends WvletToken(Keyword, "left")
  case RIGHT extends WvletToken(Keyword, "right")
  case FULL  extends WvletToken(Keyword, "full")
  case INNER extends WvletToken(Keyword, "inner")
  case CROSS extends WvletToken(Keyword, "cross")

  // column modification operators
  case ADD      extends WvletToken(Keyword, "add")
  case EXCLUDE  extends WvletToken(Keyword, "exclude")
  case RENAME   extends WvletToken(Keyword, "rename")
  case SHIFT    extends WvletToken(Keyword, "shift")
  case DROP     extends WvletToken(Keyword, "drop")
  case DESCRIBE extends WvletToken(Keyword, "describe")

  // set operators
  // Note: We do not provide UNION as a lot of users use UNION (duplicate elimination)
  // where UNION ALL is appropriate
  case CONCAT    extends WvletToken(Keyword, "concat")
  case DEDUP     extends WvletToken(Keyword, "dedup")
  case INTERSECT extends WvletToken(Keyword, "intersect")
  case EXCEPT    extends WvletToken(Keyword, "except")
  case ALL       extends WvletToken(Keyword, "all")

  // window function keywords
  case OVER      extends WvletToken(Keyword, "over")
  case PARTITION extends WvletToken(Keyword, "partition")
  case ROWS      extends WvletToken(Keyword, "rows")
  case RANGE     extends WvletToken(Keyword, "range")

  // model management keywords
  case RUN     extends WvletToken(Keyword, "run")
  case IMPORT  extends WvletToken(Keyword, "import")
  case EXPORT  extends WvletToken(Keyword, "export")
  case PACKAGE extends WvletToken(Keyword, "package")
  case MODEL   extends WvletToken(Keyword, "model")
  case EXECUTE extends WvletToken(Keyword, "execute")

  case VAL extends WvletToken(Keyword, "val")

  // Control statements
  case IF   extends WvletToken(Keyword, "if")
  case THEN extends WvletToken(Keyword, "then")
  case ELSE extends WvletToken(Keyword, "else")
  case CASE extends WvletToken(Keyword, "case")
  case WHEN extends WvletToken(Keyword, "when")
  case END  extends WvletToken(Keyword, "end")

  // Condition keywords
  case AND  extends WvletToken(Keyword, "and")
  case OR   extends WvletToken(Keyword, "or")
  case NOT  extends WvletToken(Keyword, "not")
  case IS   extends WvletToken(Keyword, "is")
  case LIKE extends WvletToken(Keyword, "like")

  // DML operators
  case SAVE     extends WvletToken(Keyword, "save")
  case APPEND   extends WvletToken(Keyword, "append")
  case DELETE   extends WvletToken(Keyword, "delete")
  case TRUNCATE extends WvletToken(Keyword, "truncate")

end WvletToken

object WvletToken:
  val keywords       = WvletToken.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = WvletToken.values.filter(_.tokenType == Op).toSeq
  val literalStartKeywords = List(
    WvletToken.NULL,
    WvletToken.TRUE,
    WvletToken.FALSE,
    WvletToken.CASE,
    WvletToken.IF,
    WvletToken.MAP
  )

  val nonReservedKeywords = Set(WvletToken.COUNT)

  val stringStartToken = List(
    WvletToken.IDENTIFIER,
    WvletToken.STRING_INTERPOLATION_PREFIX,
    WvletToken.TRIPLE_QUOTE_INTERPOLATION_PREFIX,
    WvletToken.BACKQUOTED_IDENTIFIER,
    WvletToken.SINGLE_QUOTE,
    WvletToken.DOUBLE_QUOTE
  )

  val allKeywordAndSymbol = keywords ++ literalStartKeywords ++ specialSymbols

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
      // WvletToken.TRANSFORM,
      WvletToken.TEST
    ) ++ joinKeywords

  val queryDelimiters = Set(
    WvletToken.EOF,
    WvletToken.END,
    WvletToken.R_BRACE,
    WvletToken.R_PAREN,
    WvletToken.SEMICOLON,
    WvletToken.PIPE
  )

  val stringLiterals = Set(
    WvletToken.SINGLE_QUOTE_STRING,
    WvletToken.DOUBLE_QUOTE_STRING,
    WvletToken.TRIPLE_QUOTE_STRING
  )

  val stringInterpolationPrefixes = Set(
    WvletToken.STRING_INTERPOLATION_PREFIX,
    WvletToken.TRIPLE_QUOTE_INTERPOLATION_PREFIX
  )

  def isQueryDelimiter(t: WvletToken): Boolean = queryDelimiters.contains(t)

  given tokenTypeInfo: TokenTypeInfo[WvletToken] with
    override def empty: WvletToken                        = WvletToken.EMPTY
    override def errorToken: WvletToken                   = WvletToken.ERROR
    override def eofToken: WvletToken                     = WvletToken.EOF
    override def identifier: WvletToken                   = WvletToken.IDENTIFIER
    override def findToken(s: String): Option[WvletToken] = keywordAndSymbolTable.get(s)
    override def integerLiteral: WvletToken               = WvletToken.INTEGER_LITERAL
    override def longLiteral: WvletToken                  = WvletToken.LONG_LITERAL
    override def decimalLiteral: WvletToken               = WvletToken.DECIMAL_LITERAL
    override def expLiteral: WvletToken                   = WvletToken.EXP_LITERAL
    override def doubleLiteral: WvletToken                = WvletToken.DOUBLE_LITERAL
    override def floatLiteral: WvletToken                 = WvletToken.FLOAT_LITERAL

    override def commentToken: WvletToken         = WvletToken.COMMENT
    override def singleQuoteString: WvletToken    = WvletToken.SINGLE_QUOTE_STRING
    override def doubleQuoteString: WvletToken    = WvletToken.DOUBLE_QUOTE_STRING
    override def tripleQuoteString: WvletToken    = WvletToken.TRIPLE_QUOTE_STRING
    override def whiteSpace: WvletToken           = WvletToken.WHITESPACE
    override def backQuotedIdentifier: WvletToken = WvletToken.BACKQUOTED_IDENTIFIER

end WvletToken
