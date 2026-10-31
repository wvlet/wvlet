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

import wvlet.uni.cli.LineAction
import wvlet.uni.cli.LineEditor
import wvlet.uni.cli.LineResult
import wvlet.uni.io.IO
import wvlet.uni.log.AnsiColorPalette
import wvlet.uni.util.Count
import wvlet.uni.util.ElapsedTime
import wvlet.uni.util.ThreadUtil
import wvlet.lang.api.LinePosition
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QuerySelection.All
import wvlet.lang.api.v1.query.QuerySelection.Describe
import wvlet.lang.cli.terminal.REPLTerminal
import wvlet.lang.compiler.parser.*
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceFile
import wvlet.lang.compiler.connector.QueryStats
import wvlet.lang.compiler.query.QueryMetric
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.model.plan.QueryStatement
import wvlet.lang.runner.LastOutput
import wvlet.lang.runner.WvletScriptRunner
import wvlet.uni.log.LogSupport

import java.util.concurrent.Executors
import java.util.concurrent.CancellationException
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.io.AnsiColor

/**
  * Wvlet REPL with pluggable terminal implementation
  *
  * @param runner
  *   Script runner for executing queries
  * @param terminal
  *   Terminal implementation (UniTerminal for interactive, HeadlessTerminal for non-interactive)
  */
class WvletREPL(runner: WvletScriptRunner, terminal: REPLTerminal)
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
          case stats: QueryStats =>
            val t = System.currentTimeMillis()
            // Show report every 300ms
            if t - lastUpdateTimeMillis > 300 then
              lastUpdateTimeMillis = t
              val elapsed = stats.elapsedMs.getOrElse(0L)
              val rows    = stats.rowsProcessed.getOrElse(0L)
              val done    = stats.splitsCompleted.getOrElse(0)
              val total   = stats.splitsTotal.getOrElse(0)
              val msg     =
                f"Query ${stats.state.toString.toLowerCase} ${ElapsedTime.succinctMillis(
                    elapsed
                  )}%6s [${Count.succinct(rows)} rows] ${done}/${total}"
              printLine(msg)
          case _ =>

  override def close(): Unit =
    terminal.close()
    executorThreadManager.shutdown()

  private var lastOutput: Option[LastOutput] = None

  /**
    * Run the body in a new thread, so that Ctrl-C can cancel the running query. Returns None when
    * cancelled.
    */
  private def withNewThread[Result](body: => Result): Option[Result] =
    // Register the handler before submitting, so that an early Ctrl-C is not lost
    val cancelled = AtomicBoolean(false)
    val task      = AtomicReference[Future[Result]]()
    val onCtrlC   = terminal.onInterrupt { () =>
      cancelled.set(true)
      Option(task.get()).foreach(_.cancel(true))
    }
    try
      task.set(executorThreadManager.submit(() => body))
      if cancelled.get() then
        task.get().cancel(true)
      Some(task.get().get())
    catch
      case e: CancellationException =>
        logger.error("Cancelled the query")
        None
    finally
      onCtrlC.cancel

  /**
    * Run a query from a key binding. The input is edited in raw mode, where Ctrl-C is a key, so
    * leave raw mode while the query runs to let Ctrl-C cancel it.
    */
  private def runFromKeyBinding[Result](body: => Result): Option[Result] = terminal
    .withInterruptSignal {
      withNewThread(body)
    }

  private def runStmt(trimmedLine: String): Unit =
    if trimmedLine.nonEmpty then
      withNewThread {
        val result = runner.runStatement(
          QueryRequest(query = trimmedLine, querySelection = All, isDebugRun = true)
        )
        // Display output and save for clip commands
        val output =
          if terminal.isRealTerminal then
            runner.displayOutput(trimmedLine, result, terminal.width)
          else
            // In headless mode, print to stdout
            val outputStr = result.toPrettyBox()
            if outputStr.trim.nonEmpty then
              println(outputStr)
            LastOutput(trimmedLine, outputStr, result)
        lastOutput = Some(output)
      }

  private def trimLine(line: String): String = line.trim.stripSuffix(";")

  private def moveToTop = LineAction.Custom("move-to-top", _.moveCursor(0))

  private def moveToEnd = LineAction.Custom(
    "move-to-end",
    editor => editor.moveCursor(editor.buffer.length)
  )

  private def enterStmt = LineAction.Custom(
    "enter-statement",
    editor =>
      editor.moveCursor(editor.buffer.length)
      if !editor.buffer.trim.endsWith(";") then
        editor.insert(";")
      editor.accept()
  )

  /**
    * The input up to the end of the cursor line, which is a query fragment to describe or run
    */
  private def extractQueryFragment(editor: LineEditor): String =
    val lastCursor = editor.cursor
    editor.run(LineAction.EndOfLine)
    val queryFragment = trimLine(editor.buffer.substring(0, editor.cursor))
    // Move back cursor
    editor.moveCursor(lastCursor)
    queryFragment

  private def describeLine = LineAction.Custom(
    "describe-line",
    editor =>
      // Run (query fragment) describe to show the schema
      val queryFragment = extractQueryFragment(editor)
      val lines         = queryFragment.split("\n")
      val lastLine      = lines.lastOption.getOrElse("")
      val lineNum       = lines.size
      runFromKeyBinding {
        runner.runStatement(
          QueryRequest(
            query = queryFragment,
            querySelection = Describe,
            linePosition = LinePosition(lineNum, 1),
            isDebugRun = true
          )
        )(using QueryProgressMonitor.noOp) // Hide progress for describe query
      }.foreach { result =>
        val str = result.toPrettyBox()
        editor.printAbove(
          s"${Color.GREEN}describe${Color.RESET} ${Color.BLUE}(line:${lineNum})${Color
              .RESET}: ${Color.BRIGHT_RED}${lastLine}\n${Color.GRAY}${str}${AnsiColor.RESET}"
        )
      }
  )

  private def subqueryRun = LineAction.Custom(
    "subquery-run",
    editor =>
      val queryFragment = extractQueryFragment(editor)
      terminal.addHistory(queryFragment)
      val lines         = queryFragment.split("\n")
      val lastLine      = lines.lastOption.getOrElse("")
      val lineNum       = lines.size
      val samplingQuery = s"${queryFragment}\nlimit ${runner.getResultRowLimit}"
      runFromKeyBinding {
        runner.runStatement(
          QueryRequest(query = samplingQuery, querySelection = All, isDebugRun = true)
        )(using QueryProgressMonitor.noOp) // Progress lines would break the input being edited
      }.foreach { result =>
        // The input is being edited, so print the result above it instead of paging it
        val output = runner.renderOutput(samplingQuery, result, terminal.width)
        lastOutput = Some(output)
        editor.printAbove(
          s"${Color.GREEN}debug${Color.RESET} ${Color.BLUE}(line:${lineNum})${Color.RESET}: ${Color
              .BRIGHT_RED}${lastLine}${AnsiColor.RESET}\n${output.output}"
        )
      }
  )

  /**
    * Run the given commands, or read and run inputs until the user exits
    */
  def start(commands: List[String] = Nil): Unit =
    if commands.nonEmpty then
      // If commands are given, run them in order, stopping at an exit command
      commands
        .iterator
        .map { line =>
          println(s"wv> ${line}")
          eval(line)
        }
        .takeWhile(identity)
        .foreach(_ => ())
    else
      terminal.bindKeys(
        Seq(
          "^J^A" -> moveToTop,
          "^J^E" -> moveToEnd,
          "^J^R" -> enterStmt,
          "^J^D" -> describeLine,
          "^J^T" -> subqueryRun
        )
      )
      readLoop()

  /**
    * Read and evaluate inputs until exit, Ctrl-C, or Ctrl-D on an empty line.
    *
    * Each read blocks with `await` instead of chaining reads with `Rx.flatMap`: on the JVM, the
    * reads complete synchronously, so a flatMap chain grows the stack with every input and
    * overflows in a long session.
    */
  private def readLoop(): Unit =
    var toContinue = true
    while toContinue do
      try
        terminal.readLine("wv> ").await match
          case LineResult.Line(line) =>
            toContinue = eval(line)
          case LineResult.Interrupted(_) | LineResult.EndOfInput =>
            toContinue = false
      catch
        case e: Exception =>
          // e.g., an error in a key binding. Keep the REPL running
          error(e)

  /**
    * Evaluate one input. Returns false when the REPL should exit
    */
  private def eval(line: String): Boolean =
    val trimmedLine = trimLine(line)
    val cmd         = trimmedLine.split("\\s+").headOption.getOrElse("")
    try
      cmd match
        case "exit" | "quit" =>
          false
        case "clear" =>
          terminal.clearScreen()
          true
        case "help" =>
          println(helpMessage)
          true
        case "git" | "gh" =>
          IO.run(trimmedLine)
          true
        case "clip" =>
          lastOutput match
            case Some(output) =>
              Clipboard.saveToClipboard(s"""[wv:query]\n${output.line}\n\n${output.output}""")
            case None =>
              warn("No output to clip")
          true
        case "clip-query" =>
          lastOutput match
            case Some(output) =>
              Clipboard.saveToClipboard(output.line)
            case None =>
              warn("No output to clip")
          true
        case "clip-result" =>
          lastOutput match
            case Some(output) =>
              Clipboard.saveToClipboard(output.result.toTSV)
            case None =>
              warn("No output to clip")
          true
        case "rows" =>
          val limit = trimmedLine.split("\\s+").lastOption.getOrElse("40").toInt
          if limit <= 0 then
            error("The limit must be a positive number")
          else
            runner.setResultRowLimit(limit)
            info(s"Set the result row limit to: ${limit}")
          true
        case "col-width" =>
          val width = trimmedLine.split("\\s+").lastOption.getOrElse("150").toInt
          if width <= 0 then
            error("The column width must be a positive number")
          else
            info(s"Set the column width to: ${width}")
            runner.setMaxColWidth(width)
          true
        case "context" =>
          val catalog = runner.getCurrentCatalog
          val schema  = runner.getCurrentSchema
          info(s"Current context: catalog=${catalog}, schema=${schema}")
          true
        case stmt =>
          runStmt(trimmedLine)
          true
      end match
    catch
      case e: InterruptedException =>
        false
      case e: Exception =>
        error(e)
        true
    end try

  end eval

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
    "context"
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

  private def countTripleQuotes(s: String): Int =
    s.indexOf("\"\"\"") match
      case -1 =>
        0
      case i =>
        1 + countTripleQuotes(s.substring(i + 3))

  /**
    * Whether the REPL input is a complete statement to run on Enter. Otherwise, Enter starts a new
    * line, e.g., for unclosed braces or triple-quoted strings, and queries not ending with `;`.
    */
  private[cli] def isInputComplete(line: String): Boolean =
    val cmd     = line.trim
    val cmdName = cmd.split("\\s").headOption.getOrElse("")
    // Count the number of triple quote `"""` characters to accept incomplete multiline strings
    val needsMoreString = countTripleQuotes(cmd) % 2 == 1
    val needsMoreInput  = cmd.count(_ == '{') > cmd.count(_ == '}')
    if needsMoreString then
      false
    else if cmdName.isEmpty || knownCommands.contains(cmdName) || cmd.endsWith(";") then
      true
    else if needsMoreInput then
      false
    else
      val unit = CompilationUnit.fromWvletString(line)
      try
        // Test whether the statement is a complete statement
        WvletParser(unit).statement() match
          case q: QueryStatement =>
            // QueryStatement might have additional operators, so it needs to end with ";"
            false
          case _ =>
            true
      catch
        case e: WvletLangException =>
          // Move to the secondary prompt until seeing a semicolon
          false

  end isInputComplete

  /**
    * Color keywords, literals, and comments of the REPL input with ANSI escape sequences
    */
  private[cli] def highlight(buffer: String): String =
    val builder = StringBuilder()
    val src     = SourceFile.fromWvletString(buffer)
    val scanner = WvletScanner(
      src,
      ScannerConfig(skipComments = false, skipWhiteSpace = false, reportErrorToken = true)
    )

    def colored(s: String, color: String): Unit = builder.append(s"${color}${s}${AnsiColor.RESET}")

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
            colored(rawString, AnsiColor.YELLOW)
          case WvletToken.DOC_COMMENT =>
            colored(rawString, AnsiColor.BLUE)
          case token if token.isLiteral =>
            colored(rawString, AnsiColor.GREEN)
          case token if token.isReservedKeyword =>
            colored(rawString, AnsiColor.CYAN)
          case WvletToken.IDENTIFIER =>
            colored(rawString, AnsiColor.WHITE)
          case _ =>
            builder.append(rawString)
      catch
        case e: WvletLangException if e.statusCode == StatusCode.UNCLOSED_MULTILINE_LITERAL =>
          // Skip the rest of the line
          builder.append(src.getContent.slice(lastOffset, src.getContent.length).mkString)
          toContinue = false
    end while

    builder.toString

  end highlight

end WvletREPL
