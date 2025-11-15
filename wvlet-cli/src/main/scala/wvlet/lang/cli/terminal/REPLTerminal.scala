package wvlet.lang.cli.terminal

/**
  * Terminal abstraction for REPL to avoid loading JLine3 in headless mode
  */
trait REPLTerminal extends AutoCloseable:
  /**
    * Terminal width in characters
    */
  def width: Int

  /**
    * Terminal height in lines
    */
  def height: Int

  /**
    * Set terminal size (for testing or initial setup)
    */
  def setSize(width: Int, height: Int): Unit

  /**
    * Write a string to the terminal
    */
  def write(s: String): Unit

  /**
    * Write a string followed by newline
    */
  def writeln(s: String): Unit

  /**
    * Flush the terminal output
    */
  def flush(): Unit

  /**
    * Clear the screen
    */
  def clearScreen(): Unit

  /**
    * Print a message above the current line (for interactive terminals)
    */
  def printAbove(s: String): Unit

  /**
    * Read a line of input with the given prompt
    */
  def readLine(prompt: String): String

  /**
    * Whether this is a real terminal (not dumb/headless)
    */
  def isRealTerminal: Boolean

  /**
    * Handle interrupt signal (Ctrl+C)
    */
  def handleInterrupt(handler: => Unit): Unit

  /**
    * Get the command history
    */
  def getHistory: REPLHistory

  /**
    * Execute a special REPL command (like move to top, describe line, etc.) Returns true if the
    * command was handled, false otherwise
    */
  def executeSpecialCommand(cmd: REPLCommand): Boolean

  /**
    * Get the current input buffer content (for interactive terminals)
    */
  def getBuffer: String

  /**
    * Move cursor in the input buffer to position
    */
  def moveCursor(pos: Int): Unit

  /**
    * Get current cursor position
    */
  def getCursorPosition: Int

  /**
    * Get buffer length
    */
  def getBufferLength: Int

  /**
    * Get buffer content up to cursor
    */
  def getBufferUpToCursor: String

  /**
    * Get current character at cursor
    */
  def getCurrentChar: Char

  /**
    * Write to the current buffer
    */
  def writeToBuffer(s: String): Unit

  /**
    * Call a line reader widget by name
    */
  def callWidget(widgetName: String): Unit

  /**
    * Redraw the current line
    */
  def redrawLine(): Unit

  /**
    * Get terminal output writer for advanced operations (JLine3 only)
    */
  def getOutputWriter: Option[java.io.Writer]

  /**
    * Write blank lines to the terminal output
    *
    * @param count
    *   Number of blank lines to write
    */
  def writeNewlines(count: Int): Unit

end REPLTerminal

/**
  * REPL command history abstraction
  */
trait REPLHistory:
  def add(line: String): Unit
  def save(): Unit

/**
  * Special REPL commands for interactive terminals
  */
enum REPLCommand:
  case MoveToTop
  case MoveToEnd
  case EnterStatement
  case DescribeLine
  case SubqueryRun
