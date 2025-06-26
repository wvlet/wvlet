package wvlet.lang.compiler.parser

import scala.annotation.switch

object Tokens:
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

end Tokens
