package wvlet.lang.cli.terminal

import scala.io.StdIn

/**
  * Headless terminal implementation that doesn't use JLine3
  *
  * This implementation uses simple stdin/stdout without any interactive features, ensuring JLine3
  * is never loaded in non-interactive mode.
  */
class HeadlessTerminal extends REPLTerminal:
  private var termWidth  = 120
  private var termHeight = 40
  private val history    = new HeadlessHistory()

  override def width: Int  = termWidth
  override def height: Int = termHeight

  override def setSize(width: Int, height: Int): Unit =
    termWidth = width
    termHeight = height

  override def write(s: String): Unit = print(s)

  override def writeln(s: String): Unit = println(s)

  override def flush(): Unit = Console.flush()

  override def clearScreen(): Unit =
    // No-op in headless mode
    ()

  override def printAbove(s: String): Unit =
    // In headless mode, just print normally
    println(s)

  override def readLine(prompt: String): String =
    print(prompt)
    flush()
    StdIn.readLine()

  override def isRealTerminal: Boolean = false

  override def handleInterrupt(handler: => Unit): Unit =
    // No-op in headless mode - signal handling happens at JVM level
    ()

  override def getHistory: REPLHistory = history

  override def executeSpecialCommand(cmd: REPLCommand): Boolean =
    // Special commands not supported in headless mode
    false

  override def getBuffer: String =
    // No buffer in headless mode
    ""

  override def moveCursor(pos: Int): Unit = ()

  override def getCursorPosition: Int = 0

  override def getBufferLength: Int = 0

  override def getBufferUpToCursor: String = ""

  override def getCurrentChar: Char = '\u0000'

  override def writeToBuffer(s: String): Unit = ()

  override def callWidget(widgetName: String): Unit = ()

  override def redrawLine(): Unit = ()

  override def getOutput: Any = null

  override def close(): Unit =
    // Nothing to close in headless mode
    ()

end HeadlessTerminal

/**
  * Simple in-memory history for headless mode
  */
class HeadlessHistory extends REPLHistory:
  private val entries = scala.collection.mutable.ListBuffer.empty[String]

  override def add(line: String): Unit = entries += line

  override def save(): Unit =
    // No persistence in headless mode
    ()
