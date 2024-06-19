package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.parser.FlowScanner.{InBraces, InString, Indented, Region}
import com.treasuredata.flow.lang.compiler.parser.FlowToken.{LF, SU}
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile, SourceLocation}
import com.treasuredata.flow.lang.model.NodeLocation
import wvlet.log.LogSupport

import scala.annotation.{switch, tailrec}

case class TokenData(token: FlowToken, str: String, offset: Int, length: Int):
  override def toString: String = f"[${offset}%3d:${length}%2d] ${token}%10s: ${str}"

  def sourceLocation(using unit: CompilationUnit): SourceLocation = SourceLocation(
    unit,
    nodeLocation(using unit.sourceFile)
  )

  def nodeLocation(using src: SourceFile): Option[NodeLocation] =
    val line = src.offsetToLine(offset)
    val col  = src.offsetToColumn(offset)
    Some(NodeLocation(line, col))

class ScanState(startFrom: Int = 0):
  override def toString: String = s"'${str}' <${token}> (${lastOffset}-${offset})"

  // Token type
  var token: FlowToken = FlowToken.EMPTY
  // The string value of the token
  var str: String = ""

  // The 1-character ahead offset of the last read character
  var offset: Int = startFrom
  // The offset of the character immediately before the current token
  var lastOffset: Int = startFrom
  // the offset of the newline immediately before the current token, or -1 if the current token is not the first one after a newline
  var lineOffset: Int = -1

  def copyFrom(s: ScanState): Unit =
    token = s.token
    str = s.str
    offset = s.offset
    lastOffset = s.lastOffset
    lineOffset = s.lineOffset

  def toTokenData(lastCharOffset: Int): TokenData = TokenData(
    token,
    str,
    offset,
    lastCharOffset - offset
  )

  def isAfterLineEnd: Boolean = lineOffset >= 0

case class ScannerConfig(startFrom: Int = 0, skipComments: Boolean = false)

abstract class ScannerBase(buf: IArray[Char], startFrom: Int = 0):
  import FlowToken.*

  protected var current: ScanState = ScanState(startFrom = startFrom)

  // The last read character
  protected var ch: Char = _
  // The offset +1 of the last read character
  protected var charOffset: Int = startFrom
  // The offset before the last read character
  protected var lastCharOffset: Int = startFrom
  // The start offset of the current line
  protected var lineStartOffset: Int = startFrom

  inline protected def offset: Int = current.offset
  inline private def length: Int   = buf.length

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

/**
  * Scan *.flow files
  */
class FlowScanner(source: SourceFile, config: ScannerConfig = ScannerConfig())
    extends ScannerBase(source.content, config.startFrom)
    with LogSupport:
  import FlowToken.*

  // Preserve token history
  private var prev: ScanState = ScanState(startFrom = config.startFrom)
  private var next: ScanState = ScanState(startFrom = config.startFrom)

  // Is the current token the first one after a newline?

  private val tokenBuffer           = TokenBuffer()
  private var currentRegion: Region = Indented(0, null)

  // Initialization for populating the first character
  nextChar()

  /**
    * Handle new lines. If necessary, add INDENT or OUTDENT tokens in front of the current token.
    *
    * Insert INDENT if
    *   - the indentation is significant, and
    *   - the last token can start an indentation region, and
    *   - the indentation of the current token is greater than the previous indentation width.
    */
  private def handleNewLine(): Unit =
    val indent = indentWidth(offset)
    debug(s"handle new line: ${offset}, indentWidth:${indent}")

  private def indentWidth(offset: Int): Int =
    def loop(index: Int, ch: Char): Int = 0
    loop(offset - 1, ' ')

  private def checkNoTrailingNumberSeparator(): Unit =
    if tokenBuffer.nonEmpty && isNumberSeparator(tokenBuffer.last) then
      reportError("trailing number separator", source.sourcePositionAt(offset))

  private def reportError(msg: String, loc: SourcePosition): Unit = error(s"${msg} at ${loc}")

  private def consume(expectedChar: Char): Unit =
    if ch != expectedChar then
      reportError(s"expected '${expectedChar}', but found '${ch}'", source.sourcePositionAt(offset))
    nextChar()

  def peekAhead(): Unit =
    prev.copyFrom(current)
    getNextToken(current.token)
//    if current.token == FlowToken.END && isEndMaker then
//      current.token = FlowToken.IDENTIFIER

  def shiftTokenHistory(): Unit =
    next.copyFrom(current)
    current.copyFrom(prev)

  def lookAhead(): TokenData =
    if next.token == FlowToken.EMPTY then
      peekAhead()
      shiftTokenHistory()

    next.toTokenData(lastCharOffset)

  private def putChar(ch: Char): Unit = tokenBuffer.append(ch)

  def nextToken(): TokenData =
    val lastToken = current.token
    getNextToken(lastToken)
    val t = current.toTokenData(lastCharOffset)
    debug(s"${currentRegion} ${t}")
    t

  def currentToken: TokenData = current.toTokenData(lastCharOffset)

  def inStringInterpolation: Boolean =
    currentRegion match
      case InString(_, _) =>
        true
      case _ =>
        false

  private def inMultiLineStringInterpolation: Boolean =
    currentRegion match
      case InBraces(InString(true, _)) =>
        true
      case _ =>
        false

  private def getNextToken(lastToken: FlowToken): Unit =
    // If the next token is already set, use it, otherwise fetch the next token
    if next.token == FlowToken.EMPTY then
      current.lastOffset = lastCharOffset
      currentRegion match
        case InString(multiline, _) =>
          getStringPart(multiline)
        case _ =>
          fetchToken()
    else
      current.copyFrom(next)
      next.token = FlowToken.EMPTY

  /**
    * Fetch the next token and set it to the current ScannerState
    */
  private def fetchToken(): Unit =
    current.offset = charOffset - 1
    current.lineOffset =
      if current.lastOffset < lineStartOffset then
        lineStartOffset
      else
        -1
    trace(
      s"fetchToken[${current}]: '${String.valueOf(ch)}' charOffset:${charOffset} lastCharOffset:${lastCharOffset}, lineStartOffset:${lineStartOffset}"
    )

    (ch: @switch) match
      case ' ' | '\t' | CR | LF | FF =>
        // Skip white space characters without pushing them into the buffer
        nextChar()
        fetchToken()
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' |
          'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | '_' | 'a' | 'b' |
          'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' |
          'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' =>
        putChar(ch)
        nextChar()
        getIdentRest()
        if ch == '"' && current.token == FlowToken.IDENTIFIER then
          // Switch the behavior of getDoubleQuotedString
          current.token = FlowToken.STRING_INTERPOLATION_PREFIX
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '<' | '>' | '?' | ':' | '=' | '&' | '|' |
          '\\' =>
        putChar(ch)
        nextChar()
        getOperatorRest()
      case '-' =>
        putChar(ch)
        nextChar()
        if ch == '-' then
          getLineComment()
        else
          getOperatorRest()
      case '0' =>
        var base: Int = 10
        def fetchLeadingZero(): Unit =
          putChar(ch)
          nextChar()
          ch match
            case 'x' | 'X' =>
              base = 16
              putChar(ch)
              nextChar()
            case _ =>
              base = 10
          if base != 10 && !isNumberSeparator(ch) && digit2int(ch, base) < 0 then
            error("invalid literal number")

        fetchLeadingZero()
        getNumber(base)
      case '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        getNumber(base = 10)
      case '.' =>
        nextChar()
        if '0' <= ch && ch <= '9' then
          putChar('.')
          getFraction()
          flushTokenString()
        else
          putChar('.')
          getOperatorRest()
      case '{' =>
        putChar(ch)
        nextChar()
        finishNamedToken()
      case '}' =>
        putChar(ch)
        if inMultiLineStringInterpolation then
          nextRawChar()
        else
          nextChar()
        currentRegion match
          case InBraces(outer) =>
            currentRegion = outer
          case _ =>
        finishNamedToken()
      case '\'' =>
        getSingleQuoteString()
      case '\"' =>
        getDoubleQuoteString()
      case '/' =>
        putChar(ch)
        nextChar()
        if ch == '*' then
          getBlockComment()
        else
          getOperatorRest()
      case SU =>
        current.token = FlowToken.EOF
        current.str = ""
      case _ =>
        putChar(ch)
        nextChar()
        finishNamedToken()

  private def getDoubleQuoteString(): Unit =
    if current.token == FlowToken.STRING_INTERPOLATION_PREFIX then
      currentRegion = InString(false, currentRegion)
      nextRawChar()
      if ch == '"' then
        if lookAheadChar() == '"' then
          nextRawChar()
          nextRawChar()
          // Triple quote strings
          getStringPart(multiline = true)
        else
          nextChar()
          // Empty string interpolation
          current.token = FlowToken.STRING_LITERAL
          current.str = flushTokenString()
      else
        // Single-line string interpolation
        getStringPart(multiline = false)
    else
      // Regular double quoted string
      // TODO Support unicode and escape characters
      consume('"')
      while ch != '"' && ch != SU do
        putChar(ch)
        nextChar()
      consume('"')
      current.token = FlowToken.STRING_LITERAL
      current.str = flushTokenString()

  private def getStringPart(multiline: Boolean): Unit =
    ch match
      case '"' => // end of string
        if multiline then
          nextRawChar()
          if isTripleQuote then
            current.token = FlowToken.STRING_PART
            current.str = flushTokenString()
            currentRegion = InString(multiline, currentRegion)
          else
            getStringPart(multiline)
            currentRegion = InString(multiline, currentRegion)
        else
          // Last part of the interpolated string
          nextChar()
          current.token = STRING_PART
          current.str = flushTokenString()
          currentRegion = currentRegion.outer
      case '{' =>
        lookAheadChar() match
          case '{' =>
            putChar(ch)
            nextChar()
            getStringPart(multiline)
          case _ =>
            current.token = FlowToken.STRING_PART
            current.str = flushTokenString()
            currentRegion = InBraces(currentRegion)
      case _ =>
        putChar(ch)
        nextRawChar()
        getStringPart(multiline)

  private def isTripleQuote: Boolean =
    if ch == '"' then
      nextRawChar()
      if ch == '"' then
        nextRawChar()
        while ch == '"' do
          putChar('"')
          nextChar()
        true
      else
        putChar('"')
        putChar('"')
        false
    else
      putChar('"')
      false

  private def getLineComment(): Unit =
    @tailrec
    def readToLineEnd(): Unit =
      putChar(ch)
      nextChar()
      if (ch != CR) && (ch != LF) && (ch != SU) then
        readToLineEnd()

    readToLineEnd()
    val commentLine = flushTokenString()
    if !config.skipComments then
      current.token = FlowToken.COMMENT
      current.str = commentLine
    else
      debug(s"skip comment: ${commentLine}")
      fetchToken()

  private def getBlockComment(): Unit =
    @tailrec
    def readToCommentEnd(): Unit =
      putChar(ch)
      nextChar()
      if ch == '*' then
        putChar(ch)
        nextChar()
        if ch == '/' then
          putChar(ch)
          nextChar()
        else
          readToCommentEnd()
      else
        readToCommentEnd()

    readToCommentEnd()
    current.token = FlowToken.COMMENT
    current.str = flushTokenString()

  /**
    * Skip the comment and return true if the comment is skipped
    */
  private def skipComment(): Boolean =
    @tailrec
    def skipLine(): Unit =
      nextChar()
      if (ch != CR) && (ch != LF) && (ch != SU) then
        skipLine()

    // Skip `-- line comment`
    if ch == '-' then
      skipLine()
      true
    else
      false

  private def getSingleQuoteString(): Unit =
    consume('\'')
    while ch != '\'' && ch != SU do
      putChar(ch)
      nextChar()
    consume('\'')
    current.token = FlowToken.STRING_LITERAL
    current.str = flushTokenString()

  @tailrec
  private def getIdentRest(): Unit =
    trace(s"getIdentRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' |
          'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '$' | 'a' | 'b' | 'c' |
          'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' |
          's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' | '0' | '1' | '2' | '3' | '4' | '5' | '6' |
          '7' | '8' | '9' | '_' =>
        putChar(ch)
        nextChar()
        getIdentRest()
      case _ =>
        finishNamedToken()

  @tailrec
  private def getOperatorRest(): Unit =
    trace(s"getOperatorRest[${offset}]: ch: '${String.valueOf(ch)}'")
    (ch: @switch) match
      case '~' | '!' | '@' | '#' | '%' | '^' | '*' | '+' | '-' | '<' | '>' | '?' | ':' | '=' | '&' |
          '|' | '\\' =>
        putChar(ch)
        nextChar()
        getOperatorRest()
      //    case '/' =>
      //      val nxch = lookAheadChar()
      //      if nxch == '/' || nxch == '*' then finishNamed()
      //      else {
      //        putChar(ch); nextChar(); getOperatorRest()
      //      }
      case SU =>
        finishNamedToken()
      case _ =>
        finishNamedToken()
  //      if isSpecial(ch) then {
  //        putChar(ch); nextChar(); getOperatorRest()
  //      }
  //      else if isSupplementary(ch, isSpecial) then getOperatorRest()
  //      else finishNamed()

  /**
    * Set the token string and clear the buffer
    */
  private def flushTokenString(): String =
    val str = tokenBuffer.toString
    current.str = str
    tokenBuffer.clear()
    str

  private def finishNamedToken(target: ScanState = current): Unit =
    val currentTokenStr = flushTokenString()
    trace(s"finishNamedToken at ${current}: '${currentTokenStr}'")
    val token =
      FlowToken.keywordAndSymbolTable.get(currentTokenStr) match
        case Some(tokenType) =>
          target.token = tokenType
        case None =>
          target.token = FlowToken.IDENTIFIER

  private def getNumber(base: Int): Unit =
    while isNumberSeparator(ch) || digit2int(ch, base) >= 0 do
      putChar(ch)
      nextChar()
    checkNoTrailingNumberSeparator()
    var tokenType = FlowToken.INTEGER_LITERAL

    if base == 10 && ch == '.' then
      putChar(ch)
      nextChar()
      if '0' <= ch && ch <= '9' then
        tokenType = getFraction()
    else
      (ch: @switch) match
        case 'e' | 'E' | 'f' | 'F' | 'd' | 'D' =>
          if base == 10 then
            tokenType = getFraction()
        case 'l' | 'L' =>
          nextChar()
          tokenType = FlowToken.LONG_LITERAL
        case _ =>

    checkNoTrailingNumberSeparator()

    flushTokenString()
    current.token = tokenType
  end getNumber

  private def getFraction(): FlowToken =
    var tokenType = FlowToken.DECIMAL_LITERAL
    trace(s"getFraction ch[${offset}]: '${ch}'")
    while '0' <= ch && ch <= '9' || isNumberSeparator(ch) do
      putChar(ch)
      nextChar()
    checkNoTrailingNumberSeparator()
    if ch == 'e' || ch == 'E' then
      putChar(ch)
      nextChar()
      if ch == '+' || ch == '-' then
        putChar(ch)
        nextChar()
      if '0' <= ch && ch <= '9' || isNumberSeparator(ch) then
        putChar(ch)
        nextChar()
        if ch == '+' || ch == '-' then
          putChar(ch)
          nextChar()
        while '0' <= ch && ch <= '9' || isNumberSeparator(ch) do
          putChar(ch)
          nextChar()
        checkNoTrailingNumberSeparator()
      tokenType = FlowToken.EXP_LITERAL
    if ch == 'd' || ch == 'D' then
      putChar(ch)
      nextChar()
      tokenType = FlowToken.DOUBLE_LITERAL
    else if ch == 'f' || ch == 'F' then
      putChar(ch)
      nextChar()
      tokenType = FlowToken.FLOAT_LITERAL
    // checkNoLetter()
    tokenType
  end getFraction

object FlowScanner:
  sealed trait Region:
    def outer: Region

  // Inside an interpolated string
  case class InString(multiline: Boolean, outer: Region) extends Region
  case class InBraces(outer: Region)                     extends Region
  // Inside an indented region
  case class Indented(level: Int, outer: Region | Null) extends Region
