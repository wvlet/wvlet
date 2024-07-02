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
  case Control, Literal, Identifier, Quote, Op, Keyword

import TokenType.*

import scala.annotation.switch

enum Token(val tokenType: TokenType, val str: String):
  // special tokens
  case EMPTY   extends Token(Control, "<empty>")
  case ERROR   extends Token(Control, "<erroneous token>")
  case EOF     extends Token(Control, "<eof>")
  case NEWLINE extends Token(Control, "<newline>")

  // Literals
  case INTEGER_LITERAL extends Token(Literal, "<integer literal>")
  case DECIMAL_LITERAL extends Token(Literal, "<decimal literal>")
  case EXP_LITERAL     extends Token(Literal, "<exp literal>")
  case LONG_LITERAL    extends Token(Literal, "<long literal>")
  case FLOAT_LITERAL   extends Token(Literal, "<float literal>")
  case DOUBLE_LITERAL  extends Token(Literal, "<double literal>")
  case STRING_LITERAL  extends Token(Literal, "<string literal>")
  // For interpolated string parts
  case STRING_PART extends Token(Literal, "<string part>")

  // Identifiers
  case IDENTIFIER extends Token(Identifier, "<identifier>")
  // Identifier wrapped in backquotes `....`
  case BACKQUOTED_IDENTIFIER extends Token(Identifier, "<quoted identifier>")

  // Quote characters
  case SINGLE_QUOTE extends Token(Quote, "'")
  case DOUBLE_QUOTE extends Token(Quote, "\"")
  case BACK_QUOTE   extends Token(Quote, "`")

  // Parentheses
  case L_PAREN   extends Token(Op, "(")
  case R_PAREN   extends Token(Op, ")")
  case L_BRACE   extends Token(Op, "{")
  case R_BRACE   extends Token(Op, "}")
  case L_BRACKET extends Token(Op, "[")
  case R_BRACKET extends Token(Op, "]")
  case INDENT    extends Token(Control, "<indent>")
  case OUTDENT   extends Token(Control, "<outdent>")

  // Special symbols
  case COLON      extends Token(Op, ":")
  case COMMA      extends Token(Op, ",")
  case DOT        extends Token(Op, ".")
  case UNDERSCORE extends Token(Op, "_")
  case AT         extends Token(Op, "@")
  case DOLLAR     extends Token(Op, "$")
  case STAR       extends Token(Op, "*")
  case QUESTION   extends Token(Op, "?")

  case L_ARROW        extends Token(Op, "<-")
  case R_ARROW        extends Token(Op, "->")
  case R_DOUBLE_ARROW extends Token(Op, "=>")

  // Special keywords
  case EQ   extends Token(Op, "=")
  case NEQ  extends Token(Op, "!=")
  case LT   extends Token(Op, "<")
  case GT   extends Token(Op, ">")
  case LTEQ extends Token(Op, "<=")
  case GTEQ extends Token(Op, ">=")

  case LOGICAL_AND extends Token(Op, "&&")
  case LOGICAL_OR  extends Token(Op, "||")
  case LOGICAL_NOT extends Token(Op, "!")

  case PLUS     extends Token(Op, "+")
  case MINUS    extends Token(Op, "-")
  case ASTERISK extends Token(Op, "*")
  case DIV      extends Token(Op, "/")
  case MOD      extends Token(Op, "%")

  case BITWISE_AND extends Token(Op, "&")
  case BITWISE_OR  extends Token(Op, "|")

  case HASH extends Token(Op, "#")

  // literal keywords
  case NULL  extends Token(Keyword, "null")
  case TRUE  extends Token(Keyword, "true")
  case FALSE extends Token(Keyword, "false")

  // Alphabectic keywords
  case DEF    extends Token(Keyword, "def")
  case SCHEMA extends Token(Keyword, "schema")
  case TYPE   extends Token(Keyword, "type")
  case WITH   extends Token(Keyword, "with")

  case IN extends Token(Keyword, "in")
  case BY extends Token(Keyword, "by")

  case SELECT extends Token(Keyword, "select")
  case FOR    extends Token(Keyword, "for")
  case LET    extends Token(Keyword, "let")
  case WHERE  extends Token(Keyword, "where")
  case GROUP  extends Token(Keyword, "group")
  case HAVING extends Token(Keyword, "having")
  case ORDER  extends Token(Keyword, "order")
  case JOIN   extends Token(Keyword, "join")

  // Join type keywords
  case LEFT  extends Token(Keyword, "left")
  case RIGHT extends Token(Keyword, "right")
  case CROSS extends Token(Keyword, "cross")
  case FULL  extends Token(Keyword, "full")

  case RUN    extends Token(Keyword, "run")
  case IMPORT extends Token(Keyword, "import")
  case EXPORT extends Token(Keyword, "export")

  case IF   extends Token(Keyword, "if")
  case THEN extends Token(Keyword, "then")
  case ELSE extends Token(Keyword, "else")
  case END  extends Token(Keyword, "end")

  case AND extends Token(Keyword, "and")
  case OR  extends Token(Keyword, "or")
  case NOT extends Token(Keyword, "not")

object Token:
  import Token.*
  val keywords       = Token.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = Token.values.filter(_.tokenType == Op).toSeq

  val allKeywordsAndSymbols = keywords ++ specialSymbols

  val keywordTable = allKeywordsAndSymbols.map(x => x.str -> x).toMap

  // Line feed (\n)
  inline val LF = '\u000A'
  // Form feed (\f)
  inline val FF = '\u000C'
  // Carriage return (\r)
  inline val CR = '\u000D'
  // Substitute character for representing the end of token stream
  inline val SU = '\u001A'

  def isNumberSeparator(ch: Char): Boolean = ch == '_'
  def isLineBreakChar(ch: Char): Boolean = (ch: @switch) match
    case LF | FF | CR | SU => true
    case _                 => false
