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

enum Token(val str: String):
  case EMPTY   extends Token("<empty>")
  case ERROR   extends Token("<erroneous token>")
  case EOF     extends Token("<eof>")
  case NEWLINE extends Token("<newline>")
  case INDENT  extends Token("<indent>")
  case OUTDENT extends Token("<outdent>")

  case COLON extends Token(":")
  case COMMA extends Token(",")

  case DOT          extends Token(".")
  case DOUBLE_QUOTE extends Token("\"")
  case SINGLE_QUOTE extends Token("'")

  case L_PAREN   extends Token("(")
  case R_PAREN   extends Token(")")
  case L_BRACE   extends Token("{")
  case R_BRACE   extends Token("}")
  case L_BRACKET extends Token("[")
  case R_BRACKET extends Token("]")

  case EQ   extends Token("=")
  case NEQ  extends Token("!=")
  case LT   extends Token("<")
  case GT   extends Token(">")
  case LTEQ extends Token("<=")
  case GTEQ extends Token(">=")

  case IN extends Token("in")

  case DEF    extends Token("def")
  case SCHEMA extends Token("schema")
  case WITH   extends Token("with")

  case INTEGER_LITERAL extends Token("<integer literal>")
  case DECIMAL_LITERAL extends Token("<decimal literal>")
  case EXP_LITERAL     extends Token("<exp literal>")
  case LONG_LITERAL    extends Token("<long literal>")
  case FLOAT_LITERAL   extends Token("<float literal>")
  case DOUBLE_LITERAL  extends Token("<double literal>")
  case STRING_LITERAL  extends Token("<string literal>")

  case NULL  extends Token("null")
  case TRUE  extends Token("true")
  case FALSE extends Token("false")

  case IDENTIFIER        extends Token("<identifier>")
  case QUOTED_IDENTIFIER extends Token("<quoted identifier>")

  case FOR      extends Token("for")
  case LET      extends Token("let")
  case WHERE    extends Token("where")
  case GROUP_BY extends Token("group by")
  case HAVING   extends Token("having")
  case RETURN   extends Token("return")
  case ORDER_BY extends Token("order by")

  case RUN    extends Token("run")
  case EXPORT extends Token("export")

  case IF extends Token("if")

object Tokens:
  import Token.*
  val keywords =
    Seq(NULL, TRUE, FALSE, DEF, SCHEMA, WITH, FOR, LET, WHERE, GROUP_BY, HAVING, RETURN, ORDER_BY, RUN, EXPORT, IF)
  val symbols =
    Seq(COLON, COMMA, DOT, DOUBLE_QUOTE, SINGLE_QUOTE, L_PAREN, R_PAREN, L_BRACE, R_BRACE, L_BRACKET, R_BRACKET, EQ, IN)
  val allKeywords = keywords ++ symbols
