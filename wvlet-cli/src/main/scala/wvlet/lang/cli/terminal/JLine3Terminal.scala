package wvlet.lang.cli.terminal

import org.jline.keymap.KeyMap
import org.jline.reader.*
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.DefaultParser.Bracket
import org.jline.terminal.Terminal.Signal
import org.jline.terminal.Size
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.jline.utils.AttributedString
import org.jline.utils.InfoCmp
import wvlet.lang.cli.LocalFileCompleter
import wvlet.lang.cli.WvletMain
import wvlet.lang.cli.WvletREPL
import wvlet.lang.compiler.WorkEnv

import java.io.File
import scala.jdk.CollectionConverters.*

/**
  * JLine3-based terminal implementation with full interactive features
  *
  * @param workEnv
  *   Working environment for caching history file
  */
class JLine3Terminal(workEnv: WorkEnv) extends REPLTerminal:
  private val jlineTerminal = TerminalBuilder
    .builder()
    .name("wvlet-shell")
    // Use dumb terminal for sbt testing or non-TTY environments (e.g., Claude Code, CI/CD)
    // Note: We check TTY env var instead of System.console() == null because System.console()
    // returns ProxyingConsole (not null) in Java 24+ even in non-TTY environments
    .dumb(WvletMain.isInSbt || sys.env.get("TTY").isEmpty)
    .build()

  private val historyFile = new File(workEnv.cacheFolder, ".wv_history")

  private val jlineReader = LineReaderBuilder
    .builder()
    .terminal(jlineTerminal)
    .variable(LineReader.HISTORY_FILE, historyFile.toPath)
    .parser(new WvletREPL.ReplParser())
    .completer(LocalFileCompleter(workEnv))
    // For enabling multiline input
    .variable(
      LineReader.SECONDARY_PROMPT_PATTERN,
      if isRealTerminal then
        AttributedString(s"%P  ${WvletREPL.Color.GRAY}│${WvletREPL.Color.RESET} ")
      else
        ""
    )
    .variable(LineReader.INDENTATION, 2)
    // Coloring keywords
    .highlighter(new WvletREPL.ReplHighlighter)
    .build()

  private val history = new JLine3History(jlineReader)

  override def width: Int  = jlineTerminal.getWidth
  override def height: Int = jlineTerminal.getHeight

  override def setSize(width: Int, height: Int): Unit = jlineTerminal.setSize(Size(width, height))

  override def write(s: String): Unit = jlineTerminal.writer().print(s)

  override def writeln(s: String): Unit = jlineTerminal.writer().println(s)

  override def flush(): Unit = jlineTerminal.flush()

  override def clearScreen(): Unit =
    jlineTerminal.puts(InfoCmp.Capability.clear_screen)
    jlineTerminal.flush()

  override def printAbove(s: String): Unit = jlineReader.printAbove(s)

  override def readLine(prompt: String): String = jlineReader.readLine(prompt)

  override def isRealTerminal: Boolean =
    jlineTerminal.getType != Terminal.TYPE_DUMB && jlineTerminal.getType != Terminal.TYPE_DUMB_COLOR

  override def handleInterrupt(handler: => Unit): Unit = jlineTerminal.handle(
    Signal.INT,
    _ => handler
  )

  override def getHistory: REPLHistory = history

  override def executeSpecialCommand(cmd: REPLCommand): Boolean =
    // Special commands are handled in WvletREPL
    // This method provides access to buffer operations
    cmd match
      case REPLCommand.MoveToTop =>
        val buf = jlineReader.getBuffer
        buf.cursor(0)
        true
      case REPLCommand.MoveToEnd =>
        val buf = jlineReader.getBuffer
        buf.cursor(buf.length())
        true
      case REPLCommand.EnterStatement =>
        val buf = jlineReader.getBuffer
        buf.cursor(buf.length())
        val line = buf.toString
        if !line.trim.endsWith(";") then
          buf.write(";")
          buf.cursor(buf.length())
        jlineReader.callWidget(LineReader.ACCEPT_LINE)
        true
      case _ =>
        // DescribeLine and SubqueryRun need WvletScriptRunner, handled in WvletREPL
        false

  override def getBuffer: String = jlineReader.getBuffer.toString

  override def moveCursor(pos: Int): Unit = jlineReader.getBuffer.cursor(pos)

  override def getCursorPosition: Int = jlineReader.getBuffer.cursor()

  override def getBufferLength: Int = jlineReader.getBuffer.length()

  override def getBufferUpToCursor: String = jlineReader.getBuffer.upToCursor()

  override def getCurrentChar: Char =
    val c = jlineReader.getBuffer.currChar()
    if c == -1 then
      '\u0000'
    else
      c.toChar

  override def writeToBuffer(s: String): Unit = jlineReader.getBuffer.write(s)

  override def callWidget(widgetName: String): Unit = jlineReader.callWidget(widgetName)

  override def redrawLine(): Unit = jlineReader.callWidget(LineReader.REDRAW_LINE)

  override def getOutputWriter: Option[java.io.Writer] = Some(jlineTerminal.writer())

  override def writeNewlines(count: Int): Unit =
    val writer = jlineTerminal.writer()
    for i <- 1 until count do
      writer.write('\n')

  /**
    * Get access to the JLine3 reader for advanced operations
    */
  def getReader: LineReader = jlineReader

  /**
    * Get access to the JLine3 terminal for advanced operations
    */
  def getJLineTerminal: Terminal = jlineTerminal

  /**
    * Setup interactive mode with key bindings
    */
  def setupInteractiveMode(
      moveToTop: Widget,
      moveToEnd: Widget,
      enterStmt: Widget,
      describeLine: Widget,
      subqueryRun: Widget
  ): Unit =
    // Set the default size when opening a new window or inside sbt console
    if jlineTerminal.getWidth == 0 || jlineTerminal.getHeight == 0 then
      jlineTerminal.setSize(Size(120, 40))

    // Add shortcut keys
    val keyMaps = jlineReader.getKeyMaps().get("main")

    // Clean up some default key bindings
    jlineReader
      .getKeyMaps()
      .values()
      .asScala
      .foreach { keyMap =>
        // Remove ctrl-j (accept line) to enable our custom key bindings
        keyMap.unbind(KeyMap.ctrl('J'))
      }

    // Bind shortcut keys Ctrl+J, ... sequence
    keyMaps.bind(moveToTop, KeyMap.translate("^J^A"))
    keyMaps.bind(moveToEnd, KeyMap.translate("^J^E"))
    keyMaps.bind(enterStmt, KeyMap.translate("^J^R"))
    keyMaps.bind(describeLine, KeyMap.translate("^J^D"))
    keyMaps.bind(subqueryRun, KeyMap.translate("^J^T"))

    // Load the command history so that we can use ctrl-r (keyword), ctrl+p/n (previous/next) for history search
    val history = jlineReader.getHistory
    history.attach(jlineReader)

  end setupInteractiveMode

  override def close(): Unit =
    jlineReader.getHistory.save()
    jlineTerminal.close()

end JLine3Terminal

/**
  * JLine3-based history implementation with file persistence
  */
class JLine3History(reader: LineReader) extends REPLHistory:
  override def add(line: String): Unit = reader.getHistory.add(line)

  override def save(): Unit = reader.getHistory.save()
