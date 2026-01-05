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

import org.jline.reader.*
import org.jline.reader.Parser.ParseContext
import org.jline.reader.impl.DefaultParser
import org.jline.terminal.Terminal
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStringBuilder
import org.jline.utils.AttributedStyle
import wvlet.airframe.control.Shell
import wvlet.airframe.control.ThreadUtil
import wvlet.airframe.log.AnsiColorPalette
import wvlet.airframe.metrics.Count
import wvlet.airframe.metrics.ElapsedTime
import wvlet.lang.api.LinePosition
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection.All
import wvlet.lang.api.v1.query.QuerySelection.Describe
import wvlet.lang.cli.terminal.JLine3Terminal
import wvlet.lang.cli.terminal.REPLTerminal
import wvlet.lang.compiler.parser.*
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceFile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryMetric
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.plan.QueryStatement
import wvlet.lang.runner.connector.TrinoQueryMetric
import wvlet.lang.runner.LastOutput
import wvlet.lang.runner.WvletScriptRunner
import wvlet.log.LogSupport

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import scala.io.AnsiColor

/**
  * Wvlet REPL with pluggable terminal implementation
  *
  * @param runner
  *   Script runner for executing queries
  * @param terminal
  *   Terminal implementation (JLine3 for interactive, Headless for non-interactive)
  * @param workEnv
  *   Working environment for file operations
  */
class WvletREPL(runner: WvletScriptRunner, terminal: REPLTerminal, workEnv: WorkEnv)
    extends AutoCloseable
    with LogSupport:
  import WvletREPL.*

  private val executorThreadManager = Executors.newCachedThreadPool(
    ThreadUtil.newDaemonThreadFactory("wvlet-repl-executor")
  )

  private given progressMonitor: QueryProgressMonitor =
    new QueryProgressMonitor:
      private var lines      = 0
      private val CLEAR_LINE =
        if terminal.isRealTerminal then
          "\u001b[2K"
        else
          "\r"

      private var lastUpdateTimeMillis = 0L

      private def printLine(line: String): Unit =
        if terminal.isRealTerminal then
          terminal.write(s"\r${Color.GRAY}${CLEAR_LINE}${line}${Color.RESET}")
          terminal.flush()
          lines = 1

      override def close(): Unit =
        if lines > 0 then
          terminal.write(s"${CLEAR_LINE}\r")
          terminal.flush()
          lines = 0

      override def startCompile(unit: CompilationUnit): Unit = printLine("Query compiling...")

      override def newQuery(sql: String): Unit = printLine(f"Query starting...")

      override def reportProgress(metric: QueryMetric): Unit =
        metric match
          case m: TrinoQueryMetric =>
            val t = System.currentTimeMillis()
            // Show report every 300ms
            if t - lastUpdateTimeMillis > 300 then
              lastUpdateTimeMillis = t
              val stats = m.stats
              val msg   =
                f"Query ${s"${stats.getState.toLowerCase}"} ${ElapsedTime.succinctMillis(
                    stats.getElapsedTimeMillis
                  )}%6s [${Count.succinct(stats.getProcessedRows)} rows] ${stats
                    .getCompletedSplits}/${stats.getTotalSplits}"
              printLine(msg)
          case _ =>

  override def close(): Unit =
    terminal.close()
    executorThreadManager.shutdown()

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
          // Display output and save for clip commands
          // Note: Pattern matching is necessary here because WvletScriptRunner.displayOutput()
          // requires the underlying JLine3 Terminal for advanced formatting (e.g., launching
          // less for wide output). This is a controlled use of downcasting that's safe because
          // we know JLine3Terminal is only used in interactive mode.
          val output =
            if terminal.isRealTerminal then
              terminal match
                case jline3: JLine3Terminal =>
                  runner.displayOutput(trimmedLine, result, jline3.getJLineTerminal)
                case _ =>
                  // Fallback: simple formatting without terminal features
                  LastOutput(trimmedLine, result.toPrettyBox(), result)
            else
              // In headless mode, print to stdout
              val outputStr = result.toPrettyBox()
              if outputStr.trim.nonEmpty then
                println(outputStr)
              LastOutput(trimmedLine, outputStr, result)

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
    terminal.moveCursor(0)
    true

  private def moveToEnd = newWidget: () =>
    terminal.moveCursor(terminal.getBufferLength)
    true

  private def enterStmt = newWidget: () =>
    terminal.moveCursor(terminal.getBufferLength)
    val line = terminal.getBuffer
    if !line.trim.endsWith(";") then
      terminal.writeToBuffer(";")
      terminal.moveCursor(terminal.getBufferLength)
    terminal.callWidget(LineReader.ACCEPT_LINE)
    true

  private def extractQueryFragment: String =
    val lastCursor = terminal.getCursorPosition
    if terminal.getCurrentChar != '\n' then
      terminal.callWidget(LineReader.END_OF_LINE)
    val queryFragment = trimLine(terminal.getBufferUpToCursor)
    // Move back cursor
    terminal.moveCursor(lastCursor)
    queryFragment

  private def describeLine = newWidget: () =>
    // Run (query fragment) describe to show the schema
    val queryFragment = extractQueryFragment
    val lines         = queryFragment.split("\n")
    val lastLine      = lines.lastOption.getOrElse("")
    val lineNum       = lines.size
    val result        =
      runner.runStatement(
        QueryRequest(
          query = queryFragment,
          querySelection = Describe,
          linePosition = LinePosition(lineNum, 1),
          isDebugRun = true
        )
      )(using QueryProgressMonitor.noOp) // Hide progress for describe query
    val str = result.toPrettyBox()
    terminal.printAbove(
      s"${Color.GREEN}describe${Color.RESET} ${Color.BLUE}(line:${lineNum})${Color.RESET}: ${Color
          .BRIGHT_RED}${lastLine}\n${Color.GRAY}${str}${AnsiColor.RESET}"
    )
    true

  private def subqueryRun = newWidget: () =>
    val originalQuery = terminal.getBuffer
    val queryFragment = extractQueryFragment
    terminal.getHistory.add(queryFragment)
    val lines         = queryFragment.split("\n")
    val lastLine      = lines.lastOption.getOrElse("")
    val lineNum       = lines.size
    val samplingQuery = s"${queryFragment}\nlimit ${runner.getResultRowLimit}"
    val totalLines    = originalQuery.split("\n").size
    // Need to add newlines to display the debug output in a proper position in the terminal
    println(
      s"${"\n" * (totalLines - lineNum + 1).max(0)}${Color.GREEN}debug${Color.RESET} ${Color
          .BLUE}(line:${lineNum})${Color.RESET}: ${Color.BRIGHT_RED}${lastLine}${AnsiColor.RESET}"
    )
    val result = runner.runStatement(
      QueryRequest(query = samplingQuery, querySelection = All, isDebugRun = true)
    )

    // Display output with terminal-specific formatting (see runStmt for explanation of pattern matching)
    val output =
      terminal match
        case jline3: JLine3Terminal =>
          runner.displayOutput(samplingQuery, result, jline3.getJLineTerminal)
        case _ =>
          val outputStr = result.toPrettyBox()
          if outputStr.trim.nonEmpty then
            println(outputStr)
          LastOutput(samplingQuery, outputStr, result)

    lastOutput = Some(output)

    // Add enough blank lines to redisplay the user query
    terminal.writeNewlines(lineNum - 1)

    // Redisplay the original query
    terminal.redrawLine()
    true

  def start(commands: List[String] = Nil): Unit =
    // Setup interactive mode for JLine3 terminals
    terminal match
      case jline3: JLine3Terminal =>
        jline3.setupInteractiveMode(moveToTop, moveToEnd, enterStmt, describeLine, subqueryRun)
        terminal.handleInterrupt(currentThread.get().interrupt())
      case _ =>
      // Headless mode doesn't need setup

    var toContinue = true
    while toContinue do
      def eval(line: String): Unit =
        val trimmedLine = trimLine(line)
        val cmd         = trimmedLine.split("\\s+").headOption.getOrElse("")
        cmd match
          case "exit" | "quit" =>
            toContinue = false
          case "clear" =>
            terminal.clearScreen()
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
          case "context" =>
            val catalog = runner.getCurrentCatalog
            val schema  = runner.getCurrentSchema
            info(s"Current context: catalog=${catalog}, schema=${schema}")
          case "edit" =>
            val args = trimmedLine.split("\\s+", 2)
            if args.length < 2 then
              error("Usage: edit <file>")
            else
              val filePath = args(1)
              FileEditor.editFile(filePath, workEnv)
          case "cat" =>
            val args = trimmedLine.split("\\s+", 2)
            if args.length < 2 then
              error("Usage: cat <file>")
            else
              val filePath = args(1)
              FileEditor.readFile(filePath, workEnv) match
                case Some(content) =>
                  println(content)
                case None =>
                // Error already logged by FileEditor
          case "save" =>
            val args = trimmedLine.split("\\s+", 2)
            if args.length < 2 then
              error("Usage: save <file>")
            else
              val filePath = args(1)
              lastOutput match
                case Some(output) =>
                  FileEditor.saveFile(filePath, output.line, workEnv)
                case None =>
                  error("No query to save. Run a query first.")
          case "new" =>
            val args = trimmedLine.split("\\s+", 2)
            if args.length < 2 then
              error("Usage: new <file>")
            else
              val filePath = args(1)
              FileEditor.newFile(filePath, workEnv)
          case "ls" =>
            val files = FileEditor.listWvFiles(workEnv, recursive = true)
            if files.isEmpty then
              info("No .wv files found in the working directory")
            else
              println(files.mkString("\n"))
          case stmt =>
            runStmt(trimmedLine)
        end match
      end eval

      try
        if commands.nonEmpty then
          // If a command is given, run it and exit
          for line <- commands do
            println(s"wv> ${line}")
            eval(line)
          toContinue = false
        else
          // Or read from the user input
          val line = terminal.readLine("wv> ")
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
    "col-width",
    "context",
    "edit",
    "cat",
    "save",
    "new",
    "ls"
  )

  private def helpMessage: String =
    """[commands]
      | help       : Show this help message
      | quit/exit  : Exit the REPL
      | clear      : Clear the screen
      | context    : Show current database context (catalog and schema)
      | clip       : Clip the last query and result to the clipboard
      | clip-result: Clip the last result to the clipboard in TSV format
      | clip-query : Clip the last query to the clipboard
      | rows       : Set the maximum number of query result rows to display (default: 40)
      | col-width  : Set the maximum column width to display (default: 150)
      | git        : Run a git command in the shell
      | gh         : Run a GitHub command in the shell
      |
      |[file operations]
      | edit <file>: Open a .wv file in external editor ($EDITOR, or vi/nano)
      | new <file> : Create a new .wv file and open in editor
      | cat <file> : Display file contents
      | save <file>: Save the last query to a file
      | ls         : List .wv files in the working directory
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
    *
    * Note: Visibility is `private[cli]` (not `private`) because `JLine3Terminal` needs access to
    * this class for initialization. The terminal package is a subpackage of `cli`, so
    * `private[cli]` grants access while keeping the class hidden from outside the CLI module.
    */
  private[cli] class ReplParser extends org.jline.reader.Parser with LogSupport:
    private val parser = new DefaultParser()
    parser.setEofOnUnclosedBracket(DefaultParser.Bracket.CURLY)

    // Disable escape char removal at the jline3 parser level
    override def isEscapeChar(ch: Char): Boolean = false

    override def parse(line: String, cursor: Int, context: ParseContext): ParsedLine =
      def incomplete = throw EOFError(-1, -1, null)
      def accept     = parser.parse(line, cursor, context)

      def countTripleQuotes(s: String): Int =
        s.indexOf("\"\"\"") match
          case -1 =>
            0
          case i =>
            1 + countTripleQuotes(s.substring(i + 3))

      val cmd         = line.trim
      val openBraces  = cmd.count(_ == '{')
      val closeBraces = cmd.count(_ == '}')
      // Count the number of triple quote `"""` characters to accept incomplete multiline strings
      val tripleQuotes    = countTripleQuotes(cmd)
      val needsMoreString = tripleQuotes % 2 == 1
      val needsMoreInput  = openBraces > closeBraces
      val cmdName         = cmd.split("\\s").headOption.getOrElse("")
      if needsMoreString then
        incomplete
      else if cmdName.isEmpty || knownCommands.contains(cmdName) || context == ParseContext.COMPLETE
      then
        accept
      else if cmd.endsWith(";") && cursor >= line.length then
        accept
      else if needsMoreInput then
        // Trigger indentation
        throw new EOFError(-1, -1, "missing '}'", "}", openBraces - closeBraces, "}")
      else
        val unit        = CompilationUnit.fromWvletString(line)
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
      end if

    end parse

  end ReplParser

  /**
    * Parse incomplete strings and highlight keywords
    *
    * Note: Visibility is `private[cli]` for the same reason as `ReplParser` - needed by
    * `JLine3Terminal` for syntax highlighting initialization.
    */
  private[cli] class ReplHighlighter extends org.jline.reader.Highlighter with LogSupport:
    override def highlight(reader: LineReader, buffer: String): AttributedString =
      val builder = AttributedStringBuilder()
      val src     = SourceFile.fromWvletString(buffer)
      val scanner = WvletScanner(
        src,
        ScannerConfig(skipComments = false, skipWhiteSpace = false, reportErrorToken = true)
      )

      var toContinue = true
      var lastOffset = 0
      while toContinue do
        try
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
            case WvletToken.DOC_COMMENT =>
              builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE))
            case token if token.isLiteral =>
              builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
            case token if token.isReservedKeyword =>
              builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN))
            case WvletToken.IDENTIFIER =>
              builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE))
            case _ =>
              builder.append(rawString)
        catch
          case e: WvletLangException if e.statusCode == StatusCode.UNCLOSED_MULTILINE_LITERAL =>
            // Skip the rest of the line
            builder.append(src.getContent.slice(lastOffset, src.getContent.length).mkString)
            toContinue = false
      end while

      builder.toAttributedString

    end highlight

    override def setErrorPattern(errorPattern: Pattern): Unit = {}

    override def setErrorIndex(errorIndex: Int): Unit = {}

  end ReplHighlighter

end WvletREPL
