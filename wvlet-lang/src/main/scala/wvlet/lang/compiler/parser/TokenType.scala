package wvlet.lang.compiler.parser

trait TokenTypeInfo[Token]:
  def empty: Token
  def errorToken: Token
  def eofToken: Token
  def identifier: Token
  def findToken(s: String): Option[Token]
  def integerLiteral: Token
  def longLiteral: Token
  def decimalLiteral: Token
  def expLiteral: Token
  def doubleLiteral: Token
  def floatLiteral: Token
  def commentToken: Token
  def stringLiteral: Token
  def whiteSpace: Token
