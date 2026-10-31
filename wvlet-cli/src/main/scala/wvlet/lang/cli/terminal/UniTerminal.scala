package wvlet.lang.cli.terminal

import wvlet.lang.cli.LocalFileCompleter
import wvlet.lang.cli.WvletREPL
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.cli.KeyMap
import wvlet.uni.cli.LineAction
import wvlet.uni.cli.LineReader
import wvlet.uni.cli.LineReaderConfig
import wvlet.uni.cli.LineResult
import wvlet.uni.cli.TerminalIO
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx

import java.io.File

/**
  * Interactive REPL terminal built on uni's cross-platform [[LineReader]]: emacs-style editing,
  * history saved to `.wv_history`, Ctrl-R search, local file completion, syntax highlighting, and
  * multi-line statements.
  *
  * On the JVM, line editing needs Java 22 or later. Older JVMs, and stdin/stdout that are not a
  * terminal, read input line by line.
  *
  * @param workEnv
  *   Working environment for locating the history file
  * @param io
  *   The terminal to read from; tests pass `TerminalIO.inMemory(...)`
  */
class UniTerminal(workEnv: WorkEnv, io: TerminalIO) extends REPLTerminal:
  import UniTerminal.*

  private var keyBindings = Seq.empty[(String, LineAction)]

  // Created on first use, so that key bindings registered before the first readLine are applied
  private lazy val reader: LineReader = LineReader(config, io)

  private def config: LineReaderConfig =
    val base = LineReaderConfig()
      .withHistoryFile(File(workEnv.cacheFolder, HistoryFileName).getPath)
      .withCompleter(LocalFileCompleter(workEnv))
      .withHighlighter(WvletREPL.highlight)
      .withInputComplete(WvletREPL.isInputComplete)
      .withContinuationPrompt(
        // Same width as the primary prompt `wv> `
        s"  ${WvletREPL.Color.GRAY}│${WvletREPL.Color.RESET} "
      )
      // Ctrl-J accepts the line by default. Free it for the Ctrl-J chords of the REPL
      .withKeyMap(KeyMap.emacs.unbind("^J"))
    keyBindings.foldLeft(base) { case (c, (keys, action)) =>
      c.bind(keys, action)
    }

  override def bindKeys(bindings: Seq[(String, LineAction)]): Unit = keyBindings ++= bindings

  override def width: Int = sizeOrDefault.columns

  override def height: Int = sizeOrDefault.rows

  // Some terminals (e.g., a new window or the sbt console) report 0 columns at first
  private def sizeOrDefault =
    val s = io.size
    if s.columns <= 0 || s.rows <= 0 then
      wvlet.uni.cli.TerminalSize(DefaultWidth, DefaultHeight)
    else
      s

  override def write(s: String): Unit = io.write(s)

  override def writeln(s: String): Unit = io.write(s"${s}\n")

  override def flush(): Unit = io.flush()

  override def clearScreen(): Unit =
    io.write(ClearScreen)
    io.flush()

  override def printAbove(s: String): Unit = reader.printAbove(s)

  override def readLine(prompt: String): Rx[LineResult] = reader.readLine(prompt)

  override def isRealTerminal: Boolean = io.isTTY

  override def onInterrupt(handler: () => Unit): Cancelable = io.onInterrupt(handler)

  override def addHistory(line: String): Unit = reader.history.add(line)

  // LineReader saves each history entry to the file when it is added
  override def close(): Unit = ()

end UniTerminal

object UniTerminal:
  val HistoryFileName = ".wv_history"

  private val DefaultWidth  = 120
  private val DefaultHeight = 40

  // Erase the screen and move the cursor to the top-left corner
  private val ClearScreen = "\u001b[2J\u001b[H"
