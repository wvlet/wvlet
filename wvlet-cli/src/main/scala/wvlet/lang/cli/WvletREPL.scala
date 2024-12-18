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
package wvlet.lang.cli

import org.jline.keymap.KeyMap
import org.jline.reader.*
import org.jline.reader.Parser.ParseContext
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.DefaultParser.Bracket
import org.jline.terminal.Terminal.Signal
import org.jline.terminal.{Size, Terminal, TerminalBuilder}
import org.jline.utils.{AttributedString, AttributedStringBuilder, AttributedStyle, InfoCmp, Status}
import org.jline.widget.AutopairWidgets
import wvlet.airframe.*
import wvlet.airframe.control.{Shell, ThreadUtil}
import wvlet.airframe.log.AnsiColorPalette
import wvlet.airframe.metrics.{Count, ElapsedTime}
import wvlet.lang.api.{LinePosition, WvletLangException}
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection.{All, Describe, Subquery}
import wvlet.lang.compiler.parser.*
import wvlet.lang.compiler.{CompilationUnit, SourceFile, WorkEnv}
import wvlet.lang.compiler.query.{QueryMetric, QueryProgressMonitor}
import wvlet.lang.model.plan.QueryStatement
import wvlet.lang.runner.connector.TrinoQueryMetric
import wvlet.lang.runner.{LastOutput, WvletScriptRunner}
import wvlet.log.{LogSupport, Logger}

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import scala.io.AnsiColor
import scala.jdk.CollectionConverters.*

class WvletREPL(workEnv: WorkEnv, runner: WvletScriptRunner) extends AutoCloseable with LogSupport:
  import WvletREPL.*

  private val terminal = TerminalBuilder
    .builder()
    .name("wvlet-shell")
    // Use dumb terminal for sbt testing
    .dumb(WvletMain.isInSbt).build()

  private val historyFile = new File(workEnv.cacheFolder, ".wv_history")

  private val reader = LineReaderBuilder
    .builder()
    .terminal(terminal)
    .variable(LineReader.HISTORY_FILE, historyFile.toPath)
    .parser(new ReplParser())
    // For enabling multiline input
    .variable(
      LineReader.SECONDARY_PROMPT_PATTERN,
      if isRealTerminal() then
        AttributedString(s"%P  ${Color.GRAY}│${Color.RESET} ")
      else
        ""
    )
    .variable(LineReader.INDENTATION, 2)
    .option(LineReader.Option.INSERT_BRACKET, true)
    // Coloring keywords
    .highlighter(new ReplHighlighter).build()

  private val executorThreadManager = Executors
    .newCachedThreadPool(ThreadUtil.newDaemonThreadFactory("wvlet-repl-executor"))

  private given progressMonitor: QueryProgressMonitor =
    new QueryProgressMonitor:
      private var lines = 0
      private val CLEAR_LINE =
        if isRealTerminal() then
          "\u001b[2K"
        else
          "\r"

      private var lastUpdateTimeMillis = 0L

      private def printLine(line: String): Unit =
        if isRealTerminal() then
          terminal.writer().print(s"\r${Color.GRAY}${CLEAR_LINE}${line}${Color.RESET}")
          terminal.flush()
          lines = 1

      override def close(): Unit =
        if lines > 0 then
          terminal.writer().print(s"${CLEAR_LINE}\r")
          terminal.flush()
          lines = 0

      override def startCompile(unit: CompilationUnit): Unit = printLine("Query compiling...")

      override def newQuery(sql: String): Unit = printLine(f"Query starting...")

      override def reportProgress(metric: QueryMetric): Unit =
        metric match
          case m: TrinoQueryMetric =>
            val t = System.currentTimeMillis()
            // Show report every 1s
            if t - lastUpdateTimeMillis > 300 then
              lastUpdateTimeMillis = t
              val stats = m.stats
              val msg =
                f"Query ${s"${stats.getState.toLowerCase}"} ${ElapsedTime.succinctMillis(stats.getElapsedTimeMillis)}%6s [${Count.succinct(stats.getProcessedRows)} rows] ${stats.getCompletedSplits}/${stats.getTotalSplits}"
              printLine(msg)
          case _ =>

  override def close(): Unit =
    reader.getHistory.save()
    terminal.close()
    executorThreadManager.shutdown()

  private def isRealTerminal() =
    terminal.getType != Terminal.TYPE_DUMB && terminal.getType != Terminal.TYPE_DUMB_COLOR

  private var lastOutput: Option[LastOutput] = None
  // Handle ctrl-c (int) or ctrl-d (quit) to interrupt the current thread
  private val currentThread = new AtomicReference[Thread](Thread.currentThread())

  private def withNewThread[Result](body: => Result): Result =
    val lastThread = Thread.currentThread()
    try executorThreadManager
        .submit { () =>
          currentThread.set(Thread.currentThread())
          body
        }
        .get()
    finally currentThread.set(lastThread)

  private def runStmt(trimmedLine: String): Unit =
    if trimmedLine.nonEmpty then
      withNewThread {
        try
          val result = runner.runStatement(
            QueryRequest(query = trimmedLine, querySelection = All, isDebugRun = true)
          )
          reader.getTerminal.writer()
          val output = runner.displayOutput(trimmedLine, result, terminal)
          lastOutput = Some(output)
        catch
          case e: InterruptedException =>
            logger.error("Cancelled the query")
      }

  private def trimLine(line: String): String = line.trim.stripSuffix(";")

  private def newWidget(body: () => Boolean): Widget =
    new Widget:
      override def apply(): Boolean = body()

  private def moveToTop = newWidget: () =>
    val buf = reader.getBuffer
    buf.cursor(0)
    true

  private def moveToEnd = newWidget: () =>
    val buf = reader.getBuffer
    buf.cursor(buf.length())
    true

  private def enterStmt = newWidget: () =>
    val buf = reader.getBuffer
    buf.cursor(buf.length())
    val line = buf.toString
    if !line.trim.endsWith(";") then
      buf.write(";")
      buf.cursor(buf.length())
    reader.callWidget(LineReader.ACCEPT_LINE)
    true

  private def extractQueryFragment: String =
    val buf        = reader.getBuffer
    val lastCursor = buf.cursor()
    if buf.currChar() != '\n' then
      reader.callWidget(LineReader.END_OF_LINE)
    val queryFragment = trimLine(buf.upToCursor())
    // Move back cursor
    buf.cursor(lastCursor)
    queryFragment

  private def describeLine = newWidget: () =>
    // Run (query fragment) describe to show the schema
    val queryFragment = extractQueryFragment
    val lines         = queryFragment.split("\n")
    val lastLine      = lines.lastOption.getOrElse("")
    val lineNum       = lines.size
    val result =
      runner.runStatement(
        QueryRequest(
          query = queryFragment,
          querySelection = Describe,
          linePosition = LinePosition(lineNum, 1),
          isDebugRun = true
        )
      )(using QueryProgressMonitor.noOp) // Hide progress for descirbe query
    val str = result.toPrettyBox()
    reader.printAbove(
      s"${Color.GREEN}describe${Color.RESET} ${Color.BLUE}(line:${lineNum})${Color.RESET}: ${Color.BRIGHT_RED}${lastLine}\n${Color.GRAY}${str}${AnsiColor.RESET}"
    )
    true

  private def subqueryRun = newWidget: () =>
    val originalQuery = reader.getBuffer.toString
    val queryFragment = extractQueryFragment
    reader.getHistory.add(queryFragment)
    val lines         = queryFragment.split("\n")
    val lastLine      = lines.lastOption.getOrElse("")
    val lineNum       = lines.size
    val samplingQuery = s"${queryFragment}\nlimit ${runner.getResultRowLimit}"
    val totalLines    = originalQuery.split("\n").size
    // Need to add newlines to display the debug output in a proper position in the terminal
    println(
      s"${"\n" * (totalLines - lineNum + 1).max(0)}${Color.GREEN}debug${Color.RESET} ${Color.BLUE}(line:${lineNum})${Color.RESET}: ${Color.BRIGHT_RED}${lastLine}${AnsiColor.RESET}"
    )
    val result = runner
      .runStatement(QueryRequest(query = samplingQuery, querySelection = All, isDebugRun = true))
    lastOutput = Some(runner.displayOutput(samplingQuery, result, terminal))
    val out = terminal.output()
    // Add enough blank lines to redisplay the user query
    for i <- 1 until lineNum do
      out.write('\n')

    // Redisplay the original query
    reader.callWidget(LineReader.REDRAW_LINE)
    true

  def start(commands: List[String] = Nil): Unit =
    // Set the default size when opening a new window or inside sbt console
    if terminal.getWidth == 0 || terminal.getHeight == 0 then
      terminal.setSize(Size(120, 40))

    terminal.handle(Signal.INT, _ => currentThread.get().interrupt())

    // Add shortcut keys
    val keyMaps = reader.getKeyMaps().get("main")

    import scala.jdk.CollectionConverters.*
    // Clean up some default key bindings
    reader
      .getKeyMaps()
      .values()
      .asScala
      .foreach { keyMap =>
        // Remove ctrl-j (accept line) to enable our custom key bindings
        keyMap.unbind(KeyMap.ctrl('J'))
        // Disable insert_close_curly command, which disrupts screen
        // keyMap.unbind("}")
      }

    // Bind shortcut keys Ctrl+J, ... sequence
    keyMaps.bind(moveToTop, KeyMap.translate("^J^A"))
    keyMaps.bind(moveToEnd, KeyMap.translate("^J^E"))
    keyMaps.bind(enterStmt, KeyMap.translate("^J^R"))
    keyMaps.bind(describeLine, KeyMap.translate("^J^D"))
    keyMaps.bind(subqueryRun, KeyMap.translate("^J^T"))

    // Load the command history so that we can use ctrl-r (keyword), ctrl+p/n (previous/next) for history search
    val history = reader.getHistory
    history.attach(reader)

    var toContinue = true
    while toContinue do
      def eval(line: String): Unit =
        val trimmedLine = trimLine(line)
        val cmd         = trimmedLine.split("\\s+").headOption.getOrElse("")
        cmd match
          case "exit" | "quit" =>
            toContinue = false
          case "clear" =>
            terminal.puts(InfoCmp.Capability.clear_screen)
            terminal.flush()
          case "help" =>
            println(helpMessage)
          case "git" | "gh" =>
            Shell.exec(trimmedLine)
          case "clip" =>
            lastOutput match
              case Some(output) =>
                Clipboard.saveToClipboard(s"""[wv:query]\n${output.line}\n\n${output.output}""")
              case None =>
                warn("No output to clip")
          case "clip-query" =>
            lastOutput match
              case Some(output) =>
                Clipboard.saveToClipboard(output.line)
              case None =>
                warn("No output to clip")
          case "clip-result" =>
            lastOutput match
              case Some(output) =>
                Clipboard.saveToClipboard(output.result.toTSV)
              case None =>
                warn("No output to clip")
          case "rows" =>
            val limit = trimmedLine.split("\\s+").lastOption.getOrElse("40").toInt
            if limit <= 0 then
              error("The limit must be a positive number")
            else
              runner.setResultRowLimit(limit)
              info(s"Set the result row limit to: ${limit}")
          case "col-width" =>
            val width = trimmedLine.split("\\s+").lastOption.getOrElse("150").toInt
            if width <= 0 then
              error("The column width must be a positive number")
            else
              info(s"Set the column width to: ${width}")
              runner.setMaxColWidth(width)
          case stmt =>
            runStmt(trimmedLine)
        end match
      end eval

      try
        if commands.nonEmpty then
          // If a command is given, run it and exist
          for line <- commands do
            println(s"wv> ${line}")
            eval(line)
          toContinue = false
        else
          // Or read from the user input
          val line = reader.readLine("wv> ")
          eval(line)
      catch
        case e: UserInterruptException =>
          toContinue = false
        case e: EndOfFileException =>
          toContinue = false
        case e: InterruptedException =>
          toContinue = false
        case e: Exception =>
          error(e)
    end while
  end start

end WvletREPL

object WvletREPL extends LogSupport:

  private def knownCommands = Set(
    "exit",
    "quit",
    "clear",
    "help",
    "git",
    "gh",
    "clip",
    "clip-result",
    "clip-query",
    "rows",
    "col-width"
  )

  private def helpMessage: String =
    """[commands]
      | help       : Show this help message
      | quit/exit  : Exit the REPL
      | clear      : Clear the screen
      | clip       : Clip the last query and result to the clipboard
      | clip-result: Clip the last result to the clipboard in TSV format
      | clip-query : Clip the last query to the clipboard
      | rows       : Set the maximum number of query result rows to display (default: 40)
      | col-width  : Set the maximum column width to display (default: 150)
      | git        : Run a git command in the shell
      | gh         : Run a GitHub command in the shell
      |""".stripMargin

  object Keys:
    val Alt: String = "\u001b"
    val ShiftUp     = Alt + "[1;2A"
    val ShiftDown   = Alt + "[1;2B"
    val ShiftRight  = Alt + "[1;2C"
    val ShiftLeft   = Alt + "[1;2D"
    val AltUp       = Alt * 2 + "[A"
    val AltDown     = Alt * 2 + "[B"
    val AltRight    = Alt * 2 + "[C"
    val AltLeft     = Alt * 2 + "[D"

  object Color extends AnsiColorPalette

  /**
    * A custom parser to enable receiving multiline inputs in REPL
    */
  private class ReplParser extends org.jline.reader.Parser with LogSupport:
    private val parser = new DefaultParser()
    parser.setEofOnUnclosedBracket(DefaultParser.Bracket.CURLY)

    // Disable escape char removal at the jline3 parser level
    override def isEscapeChar(ch: Char): Boolean = false

    override def parse(line: String, cursor: Int, context: ParseContext): ParsedLine =
      def incomplete = throw EOFError(-1, -1, null)
      def accept     = parser.parse(line, cursor, context)

      val cmd     = line.trim
      val cmdName = cmd.split("\\s").headOption.getOrElse("")
      if cmdName.isEmpty || knownCommands.contains(cmdName) || context == ParseContext.COMPLETE then
        accept
      else if cmd.endsWith(";") && cursor >= line.length then
        accept
      else
        val unit        = CompilationUnit.fromString(line)
        val wvletParser = WvletParser(unit)
        try
          // Test whether the statement is a complete statement
          val stmt = wvletParser.statement()
          stmt match
            case q: QueryStatement =>
              // QueryStatement might have additional operators, so it needs to end with ";"
              incomplete
            case _ =>
              if cursor >= line.length then
                // Accept model only when the cursor is at the end of the input
                accept
              else
                incomplete
        catch
          case e: WvletLangException =>
            // Move to the secondary prompt until seeing a semicolon
            incomplete

    end parse

  end ReplParser

  /**
    * Parse incomplete strings and highlight keywords
    */
  private class ReplHighlighter extends org.jline.reader.Highlighter with LogSupport:
    override def highlight(reader: LineReader, buffer: String): AttributedString =
      val builder = AttributedStringBuilder()
      val src     = SourceFile.fromString(buffer)
      val scanner = WvletScanner(
        src,
        ScannerConfig(skipComments = false, skipWhiteSpace = false, reportErrorToken = true)
      )

      var toContinue = true
      var lastOffset = 0
      while toContinue do
        val t = scanner.nextToken()

        // Extract the raw string between the last offset and the current token
        val rawString: String = src.getContent.slice(lastOffset, t.offset + t.length).mkString
        lastOffset = t.offset + t.length

        t.token match
          case WvletToken.EOF =>
            toContinue = false
          case WvletToken.ERROR =>
            builder.append(rawString)
          case WvletToken.COMMENT =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
          case token if token.isLiteral =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
          case token if token.isReservedKeyword =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN))
          case WvletToken.IDENTIFIER =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE))
          case _ =>
            builder.append(rawString)
      builder.toAttributedString

    end highlight

    override def setErrorPattern(errorPattern: Pattern): Unit = {}

    override def setErrorIndex(errorIndex: Int): Unit = {}

  end ReplHighlighter

end WvletREPL
