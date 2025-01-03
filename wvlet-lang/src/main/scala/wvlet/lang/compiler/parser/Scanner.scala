package wvlet.lang.compiler.parser

import wvlet.lang.api.{LinePosition, SourceLocation, Span, StatusCode}
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.ContextUtil.*
import wvlet.lang.compiler.parser.WvletScanner.{Indented, Region}
import wvlet.log.LogSupport

case class TokenData[Token](token: Token, str: String, offset: Int, length: Int):
  override def toString: String = f"[${offset}%3d:${length}%2d] ${token}%10s: ${str}"

  def sourceLocation(using unit: CompilationUnit): SourceLocation = unit
    .sourceLocationAt(nodeLocation(using unit.sourceFile))

  def span: Span = Span(offset, offset + length, 0)

  def nodeLocation(using src: SourceFile): LinePosition =
    val line = src.offsetToLine(offset)
    val col  = src.offsetToColumn(offset)
    LinePosition(line + 1, col)

end TokenData

class ScanState[Token](var token: Token, startFrom: Int = 0):
  override def toString: String = s"'${str}' <${token}> (${lastOffset}-${offset})"

  // The string value of the token
  var str: String = ""

  // The 1-character ahead offset of the last read character
  var offset: Int = startFrom
  // The offset of the character immediately before the current token
  var lastOffset: Int = startFrom
  // the offset of the newline immediately before the current token, or -1 if the current token is not the first one after a newline
  var lineOffset: Int = -1

  def copyFrom(s: ScanState[Token]): Unit =
    token = s.token
    str = s.str
    offset = s.offset
    lastOffset = s.lastOffset
    lineOffset = s.lineOffset

  def toTokenData(lastCharOffset: Int): TokenData[Token] = TokenData(
    token,
    str,
    offset,
    lastCharOffset - offset
  )

  def isAfterLineEnd: Boolean = lineOffset >= 0
end ScanState

case class ScannerConfig(
    startFrom: Int = 0,
    skipComments: Boolean = false,
    skipWhiteSpace: Boolean = true,
    reportErrorToken: Boolean = false,
    debugScanner: Boolean = false
)

abstract class ScannerBase[Token](sourceFile: SourceFile, config: ScannerConfig)(using
    tokenType: TokenTypeInfo[Token]
) extends LogSupport:
  import Tokens.*

  protected val buf: IArray[Char]         = sourceFile.getContent
  protected var current: ScanState[Token] = ScanState(tokenType.empty, config.startFrom)

  // Preserve token history
  protected var prev: ScanState[Token] = ScanState(tokenType.empty, startFrom = config.startFrom)

  protected var next: ScanState[Token] = ScanState(tokenType.empty, startFrom = config.startFrom)

  // Is the current token the first one after a newline?

  protected val tokenBuffer           = TokenBuffer()
  protected var currentRegion: Region = Indented(0, null)

  // The last read character
  protected var ch: Char = _
  // The offset +1 of the last read character
  protected var charOffset: Int = config.startFrom
  // The offset before the last read character
  protected var lastCharOffset: Int = config.startFrom
  // The start offset of the current line
  protected var lineStartOffset: Int = config.startFrom

  inline protected def offset: Int = current.offset
  inline private def length: Int   = buf.length

  // Initialization for populating the first character
  nextChar()

  protected def nextChar(): Unit =
    val index = charOffset
    lastCharOffset = index
    charOffset = index + 1
    if index >= length then
      // Set SU to represent the end of the file
      ch = SU
    else
      val c = buf(index)
      ch = c
      if c < ' ' then
        fetchLineEnd()

  protected def nextRawChar(): Unit =
    val index = charOffset
    lastCharOffset = index
    charOffset = index + 1
    if index >= length then
      ch = SU
    else
      ch = buf(index)

  private def fetchLineEnd(): Unit =
    // Handle CR LF as a single LF
    if ch == CR then
      if charOffset < length && buf(offset) == LF then
        current.offset += 1
        ch = LF

    // Found a new line. Update the line start offset
    if ch == LF || ch == FF then
      lineStartOffset = charOffset

  protected def lookAheadChar(): Char =
    val index = charOffset
    if index >= length then
      SU
    else
      buf(index)

  protected def checkNoTrailingNumberSeparator(): Unit =
    if tokenBuffer.nonEmpty && isNumberSeparator(tokenBuffer.last) then
      reportError("trailing number separator", offset)

  protected def reportError(msg: String, offset: Int): Unit =
    if config.reportErrorToken then
      throw StatusCode.UNEXPECTED_TOKEN.newException(msg)
    else
      val loc = sourceFile.sourceLocationAt(offset)
      error(s"${msg} at ${loc}")

  protected def consume(expectedChar: Char): Unit =
    if ch != expectedChar then
      reportError(s"expected '${expectedChar}', but found '${ch}'", offset)
    nextChar()

  protected def shiftTokenHistory(): Unit =
    next.copyFrom(current)
    current.copyFrom(prev)

  protected def putChar(ch: Char): Unit = tokenBuffer.append(ch)

  protected def getNextToken(lastToken: Token): Unit

  def currentToken: TokenData[Token] = current.toTokenData(lastCharOffset)

  def peekAhead(): Unit =
    prev.copyFrom(current)
    getNextToken(current.token)

  def lookAhead(): TokenData[Token] =
    if next.token == tokenType.empty then
      peekAhead()
      shiftTokenHistory()
    next.toTokenData(lastCharOffset)

end ScannerBase
