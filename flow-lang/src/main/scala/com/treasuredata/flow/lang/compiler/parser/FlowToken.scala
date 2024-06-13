package com.treasuredata.flow.lang.compiler.parser

enum TokenType:
  case Control, Doc, Literal, Identifier, Quote, Op, Keyword

import TokenType.*

import scala.annotation.switch

enum FlowToken(val tokenType: TokenType, val str: String):
  // special tokens
  case EMPTY   extends FlowToken(Control, "<empty>")
  case ERROR   extends FlowToken(Control, "<erroneous token>")
  case EOF     extends FlowToken(Control, "<eof>")
  case NEWLINE extends FlowToken(Control, "<newline>")

  // doc or comments
  case COMMENT extends FlowToken(Doc, "<comment>")

  // For indentation
  case INDENT  extends FlowToken(Control, "<indent>")
  case OUTDENT extends FlowToken(Control, "<outdent>")

  // Literals
  case INTEGER_LITERAL extends FlowToken(Literal, "<integer literal>")
  case DECIMAL_LITERAL extends FlowToken(Literal, "<decimal literal>")
  case EXP_LITERAL     extends FlowToken(Literal, "<exp literal>")
  case LONG_LITERAL    extends FlowToken(Literal, "<long literal>")
  case FLOAT_LITERAL   extends FlowToken(Literal, "<float literal>")
  case DOUBLE_LITERAL  extends FlowToken(Literal, "<double literal>")
  case STRING_LITERAL  extends FlowToken(Literal, "<string literal>")

  // For interpolated string, e.g., sql"...${expr}..."
  case STRING_INTERPOLATION extends FlowToken(Literal, "<string interpolation>")
  case STRING_PART          extends FlowToken(Literal, "<string part>")

  // Identifiers
  case IDENTIFIER extends FlowToken(Identifier, "<identifier>")
  // Identifier wrapped in backquotes `....`
  case BACKQUOTED_IDENTIFIER extends FlowToken(Identifier, "<quoted identifier>")

  case SINGLE_QUOTE extends FlowToken(Quote, "'")
  case DOUBLE_QUOTE extends FlowToken(Quote, "\"")

  // Parentheses
  case L_PAREN   extends FlowToken(Op, "(")
  case R_PAREN   extends FlowToken(Op, ")")
  case L_BRACE   extends FlowToken(Op, "{")
  case R_BRACE   extends FlowToken(Op, "}")
  case L_BRACKET extends FlowToken(Op, "[")
  case R_BRACKET extends FlowToken(Op, "]")

  // Special symbols
  case COLON      extends FlowToken(Op, ":")
  case COMMA      extends FlowToken(Op, ",")
  case DOT        extends FlowToken(Op, ".")
  case UNDERSCORE extends FlowToken(Op, "_")
  case AT         extends FlowToken(Op, "@")
  case DOLLAR     extends FlowToken(Op, "$")
  case STAR       extends FlowToken(Op, "*")
  case QUESTION   extends FlowToken(Op, "?")

  case L_ARROW        extends FlowToken(Op, "<-")
  case R_ARROW        extends FlowToken(Op, "->")
  case R_DOUBLE_ARROW extends FlowToken(Op, "=>")

  // Special keywords
  case EQ   extends FlowToken(Op, "=")
  case NEQ  extends FlowToken(Op, "!=")
  case LT   extends FlowToken(Op, "<")
  case GT   extends FlowToken(Op, ">")
  case LTEQ extends FlowToken(Op, "<=")
  case GTEQ extends FlowToken(Op, ">=")

  case PLUS  extends FlowToken(Op, "+")
  case MINUS extends FlowToken(Op, "-")
  case DIV   extends FlowToken(Op, "/")
  case MOD   extends FlowToken(Op, "%")

  case EXCLAMATION extends FlowToken(Op, "!")

  case AMP  extends FlowToken(Op, "&")
  case PIPE extends FlowToken(Op, "|")

  case HASH extends FlowToken(Op, "#")

  // literal keywords
  case NULL  extends FlowToken(Keyword, "null")
  case TRUE  extends FlowToken(Keyword, "true")
  case FALSE extends FlowToken(Keyword, "false")

  // Alphabectic keywords
  case DEF    extends FlowToken(Keyword, "def")
  case SCHEMA extends FlowToken(Keyword, "schema")
  case TYPE   extends FlowToken(Keyword, "type")
  case WITH   extends FlowToken(Keyword, "with")
  case TEST   extends FlowToken(Keyword, "test")

  case THIS  extends FlowToken(Keyword, "this")
  case USING extends FlowToken(Keyword, "using")

  case IN extends FlowToken(Keyword, "in")
  case BY extends FlowToken(Keyword, "by")
  case AS extends FlowToken(Keyword, "as")

  case FROM   extends FlowToken(Keyword, "from")
  case SELECT extends FlowToken(Keyword, "select")
  case FOR    extends FlowToken(Keyword, "for")
  case LET    extends FlowToken(Keyword, "let")
  case WHERE  extends FlowToken(Keyword, "where")
  case GROUP  extends FlowToken(Keyword, "group")
  case HAVING extends FlowToken(Keyword, "having")
  case ORDER  extends FlowToken(Keyword, "order")
  case JOIN   extends FlowToken(Keyword, "join")

  case RUN     extends FlowToken(Keyword, "run")
  case IMPORT  extends FlowToken(Keyword, "import")
  case EXPORT  extends FlowToken(Keyword, "export")
  case PACKAGE extends FlowToken(Keyword, "package")
  case MODEL   extends FlowToken(Keyword, "model")

  case IF   extends FlowToken(Keyword, "if")
  case THEN extends FlowToken(Keyword, "then")
  case ELSE extends FlowToken(Keyword, "else")
  case END  extends FlowToken(Keyword, "end")

  case AND extends FlowToken(Keyword, "and")
  case OR  extends FlowToken(Keyword, "or")
  case NOT extends FlowToken(Keyword, "not")

object FlowToken:
  val keywords       = FlowToken.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = FlowToken.values.filter(_.tokenType == Op).toSeq

  val allKeywordAndSymbol = keywords ++ specialSymbols

  val keywordAndSymbolTable = allKeywordAndSymbol.map(x => x.str -> x).toMap

  // Line Feed '\n'
  inline val LF = '\u000A'
  // Form Feed '\f'
  inline val FF = '\u000C'
  // Carriage Return '\r'
  inline val CR = '\u000D'
  // Substitute (SUB), which is used as the EOF marker in Windows
  inline val SU = '\u001A'

  def isLineBreakChar(c: Char): Boolean = (c: @switch) match
    case LF | FF | CR | SU => true
    case _                 => false

  /**
    * White space character but not a new line (\n)
    *
    * @param c
    * @return
    */
  def isWhiteSpaceChar(c: Char): Boolean = (c: @switch) match
    case ' ' | '\t' | CR => true
    case _               => false

  def isNumberSeparator(ch: Char): Boolean = ch == '_'

  /**
    * Convert a character to an integer value using the given base. Returns -1 upon failures
    */
  def digit2int(ch: Char, base: Int): Int =
    val num =
      if ch <= '9' then ch - '0'
      else if 'a' <= ch && ch <= 'z' then ch - 'a' + 10
      else if 'A' <= ch && ch <= 'Z' then ch - 'A' + 10
      else -1
    if 0 <= num && num < base then num
    else -1
