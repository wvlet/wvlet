package wvlet.lang.compiler.parser

trait TokenTypeInfo[Token]:
  def empty: Token
  def errorToken: Token
  def eofToken: Token
  def identifier: Token
  def findToken(s: String): Option[Token]
