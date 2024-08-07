package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.BuildInfo
import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.parser.*
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import com.treasuredata.flow.lang.model.plan.{ModelDef, Query}
import com.treasuredata.flow.lang.runner.connector.DBContext
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBContext
import com.treasuredata.flow.lang.runner.connector.trino.{TrinoConfig, TrinoContext}
import org.jline.reader.Parser.ParseContext
import org.jline.reader.impl.DefaultParser
import org.jline.reader.*
import org.jline.terminal.Terminal.Signal
import org.jline.terminal.{Size, Terminal, TerminalBuilder}
import org.jline.utils.{AttributedString, AttributedStringBuilder, AttributedStyle, InfoCmp}
import wvlet.airframe.*
import wvlet.airframe.control.{CommandLineTokenizer, Shell, ThreadUtil}
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.log.{LogSupport, Logger}

import java.io.{BufferedWriter, File, OutputStreamWriter}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

object FlowREPLCli:

  private def launcher: Launcher = Launcher.of[FlowREPLCli]

  def main(args: Array[String]): Unit = launcher.execute(args)

  def main(argLine: String): Unit = launcher.execute(argLine)

class FlowREPLCli(
    opts: FlowCliOption,
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "-c", description = "Run a command and exit")
    commands: List[String] = Nil,
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
) extends LogSupport:
  Logger("com.treasuredata.flow.lang.runner").setLogLevel(opts.logLevel)

  @command(description = "Show the version")
  def version: Unit = info(s"treasure-flow version: ${BuildInfo.version}")

  @command(description = "Start a REPL", isDefault = true)
  def repl(): Unit =
    val currentProfile: Option[Profile] = profile.flatMap { targetProfile =>
      Profile.getProfile(targetProfile) match
        case Some(p) =>
          debug(s"Using profile: ${targetProfile}")
          Some(p)
        case None =>
          error(s"No profile ${targetProfile} found")
          None
    }

    val selectedCatalog = catalog.orElse(currentProfile.flatMap(_.catalog))
    val selectedSchema  = schema.orElse(currentProfile.flatMap(_.schema))

    val design = Design
      .newSilentDesign
      .bindSingleton[FlowREPL]
      .bindInstance[FlowScriptRunnerConfig](
        FlowScriptRunnerConfig(
          workingFolder = workFolder,
          interactive = commands.isEmpty,
          catalog = selectedCatalog,
          schema = selectedSchema
        )
      )
      .bindInstance[DBContext] {
        currentProfile match
          case Some(p) if p.`type` == "trino" =>
            TrinoContext(
              TrinoConfig(
                catalog = selectedCatalog.getOrElse("default"),
                schema = selectedSchema.getOrElse("default"),
                hostAndPort = p.host.getOrElse("localhost"),
                user = p.user,
                password = p.password
              )
            )
          case _ =>
            DuckDBContext()
      }

    design.build[FlowREPL] { repl =>
      repl.start(commands)
    }
  end repl

end FlowREPLCli

class FlowREPL(runner: FlowScriptRunner) extends AutoCloseable with LogSupport:
  import FlowREPL.*

  private val terminal         = TerminalBuilder.builder().name("Treasure Flow").build()
  private val historyFile      = new File(sys.props("user.home"), ".cache/flow/.flow_history")
  private val isMacOS: Boolean = sys.props.get("os.name").exists(_.toLowerCase.contains("mac"))

  private val reader = LineReaderBuilder
    .builder()
    .terminal(terminal)
    .variable(LineReader.HISTORY_FILE, historyFile.toPath)
    .parser(new ReplParser())
    // For enabling multiline input
    .variable(
      LineReader.SECONDARY_PROMPT_PATTERN,
      if isRealTerminal() then
        AttributedString("%P  | ", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT))
      else
        ""
    )
    // Coloring keywords
    .highlighter(new ReplHighlighter).build()

  private val executorThreadManager = Executors
    .newCachedThreadPool(ThreadUtil.newDaemonThreadFactory("flow-repl-executor"))

  override def close(): Unit =
    reader.getHistory.save()
    terminal.close()
    executorThreadManager.shutdown()

  private def isRealTerminal() =
    terminal.getType != Terminal.TYPE_DUMB && terminal.getType != Terminal.TYPE_DUMB_COLOR

  def start(commands: List[String] = Nil): Unit =
    // Set the default size when opening a new window
    if terminal.getWidth == 0 || terminal.getHeight == 0 then
      terminal.setSize(Size(120, 40))

    // Handle ctrl-c (int) or ctrl-d (quit) to interrupt the current thread
    val currentThread = new AtomicReference[Thread](Thread.currentThread())
    def withNewThread[Result](body: => Result): Result =
      val lastThread = Thread.currentThread()
      try executorThreadManager
          .submit { () =>
            currentThread.set(Thread.currentThread())
            body
          }
          .get()
      finally currentThread.set(lastThread)

    terminal.handle(Signal.INT, _ => currentThread.get().interrupt())

    // Load the command history so that we can use ctrl-r (keyword), ctrl+p/n (previous/next) for history search
    val history = reader.getHistory
    history.attach(reader)

    var toContinue                     = true
    var lastOutput: Option[LastOutput] = None
    while toContinue do
      def eval(line: String): Unit =
        val trimmedLine = line.trim.stripSuffix(";")
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
                // Save query and result to clipboard (only for Mac OS, now)
                if isMacOS then
                  val proc = ProcessUtil.launchInteractiveProcess("pbcopy")
                  val out  = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()))
                  out.write(s"""[flow:query]\n${output.line}\n\n${output.output}""")
                  out.flush()
                  out.close()
                  proc.waitFor()
                  info(s"Clipped the output to the clipboard")
                else
                  warn("clip command is not supported other than Mac OS")
              case None =>
                warn("No output to clip")
          case "rows" =>
            val limit = trimmedLine.split("\\s+").lastOption.getOrElse("40").toInt
            if limit <= 0 then
              error("The limit must be a positive number")
            else
              runner.setResultRowLimit(limit)
              info(s"Set the result row limit to: ${limit}")
          case stmt =>
            if trimmedLine.nonEmpty then
              withNewThread {
                try
                  val result = runner.runStatement(trimmedLine, terminal)
                  lastOutput = Some(result)
                catch
                  case e: InterruptedException =>
                    logger.error("Cancelled the query")
              }
        end match
      end eval

      try
        if commands.nonEmpty then
          // If a command is given, run it and exist
          for line <- commands do
            println(s"flow> ${line}")
            eval(line)
          toContinue = false
        else
          // Or read from the user input
          val line = reader.readLine("flow> ")
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

end FlowREPL

object FlowREPL:
  private def knownCommands = Set("exit", "quit", "clear", "help", "git", "gh", "clip", "rows")
  private def helpMessage: String =
    """[commands]
      | help      : Show this help message
      | quit/exit : Exit the REPL
      | clear     : Clear the screen
      | clip      : Clip the current result to the clipboard
      | rows      : Set the maximum number of query result rows to display (default: 40)
      |""".stripMargin

  /**
    * A custom parser to enable receiving multiline inputs in REPL
    */
  private class ReplParser extends org.jline.reader.Parser with LogSupport:
    private val parser = new DefaultParser()

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
        val unit       = CompilationUnit.fromString(line)
        val flowParser = FlowParser(unit)
        try
          // Test whether the statement is a complete statement
          val stmt = flowParser.statement()
          stmt match
            case q: Query =>
              // Query might have additional operators, so it needs to end with ";"
              incomplete
            case _ =>
              if cursor >= line.length then
                // Accept model only when the cursor is at the end of the input
                accept
              else
                incomplete
        catch
          case e: FlowLangException =>
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
      val scanner = FlowScanner(
        src,
        ScannerConfig(skipComments = false, skipWhiteSpace = false, reportErrorToken = true)
      )

      var toContinue = true
      while toContinue do
        val t = scanner.nextToken()

        def rawString: String = src.content.slice(t.offset, t.offset + t.length).mkString

        t.token match
          case FlowToken.EOF =>
            toContinue = false
          case FlowToken.ERROR =>
            builder.append(rawString)
          case FlowToken.COMMENT =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
          case FlowToken.IDENTIFIER =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE))
          case token if token.tokenType == TokenType.Literal =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
          case token if token.tokenType == TokenType.Keyword =>
            builder.append(rawString, AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN))
          case _ =>
            builder.append(rawString)
      builder.toAttributedString

    end highlight

    override def setErrorPattern(errorPattern: Pattern): Unit = {}

    override def setErrorIndex(errorIndex: Int): Unit = {}

  end ReplHighlighter

end FlowREPL
