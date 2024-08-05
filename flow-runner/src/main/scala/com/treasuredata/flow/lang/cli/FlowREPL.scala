package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.BuildInfo
import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{CompilationUnit, SourceFile}
import com.treasuredata.flow.lang.compiler.parser.{
  FlowParser,
  FlowScanner,
  FlowToken,
  ScannerConfig,
  TokenType
}
import com.treasuredata.flow.lang.model.plan.Query
import com.treasuredata.flow.lang.runner.connector.DBContext
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBContext
import com.treasuredata.flow.lang.runner.connector.trino.{TrinoConfig, TrinoContext}
import org.jline.builtins.SyntaxHighlighter
import org.jline.reader.Parser.ParseContext
import org.jline.reader.impl.{DefaultHighlighter, DefaultParser}
import org.jline.reader.{
  EOFError,
  EndOfFileException,
  LineReader,
  LineReaderBuilder,
  UserInterruptException
}
import org.jline.terminal.Terminal.Signal
import org.jline.terminal.{Size, Terminal, TerminalBuilder}
import org.jline.utils.{AttributedString, AttributedStringBuilder, AttributedStyle, InfoCmp}
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.log.{LogSupport, Logger}
import wvlet.airframe.*

import java.io.File
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
    workFolder: String = "."
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

    val design = Design
      .newSilentDesign
      .bindSingleton[FlowREPL]
      .bindInstance[FlowScriptRunnerConfig](
        FlowScriptRunnerConfig(workingFolder = workFolder, interactive = commands.isEmpty)
      )
      .bindInstance[DBContext] {
        currentProfile.flatMap(_.connector.headOption) match
          case Some(connector) if connector.`type` == "trino" =>
            TrinoContext(
              TrinoConfig(
                catalog = connector.database,
                schema = connector.schema.getOrElse("default"),
                hostAndPort = connector.host.getOrElse("localhost"),
                user = connector.user,
                password = connector.password
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

  private val terminal    = TerminalBuilder.builder().name("Treasure Flow").build()
  private val historyFile = new File(sys.props("user.home"), ".cache/flow/.flow_history")

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

  override def close(): Unit =
    reader.getHistory.save()
    terminal.close()

  private def isRealTerminal() =
    terminal.getType != Terminal.TYPE_DUMB && terminal.getType != Terminal.TYPE_DUMB_COLOR

  def start(commands: List[String] = Nil): Unit =
    // Set the default size when opening a new window
    if terminal.getWidth == 0 || terminal.getHeight == 0 then
      terminal.setSize(Size(120, 40))

    // Handle ctrl-c (int) or ctrl-d (quit) to interrupt the current thread
    val currentThread = Thread.currentThread()
    terminal.handle(Signal.INT, _ => currentThread.interrupt())

    // Load the command history so that we can use ctrl-r (keyword), ctrl+p/n (previous/next) for history search
    val history = reader.getHistory
    history.attach(reader)

    var toContinue = true
    while toContinue do
      def eval(line: String): Unit =
        val cmd = line.trim.stripSuffix(";")
        cmd match
          case "exit" | "quit" =>
            toContinue = false
          case "clear" =>
            terminal.puts(InfoCmp.Capability.clear_screen)
            terminal.flush()
          case "help" =>
            println(helpMessage)
          case stmt =>
            if stmt.nonEmpty then
              runner.runStatement(stmt, terminal)

      try
        // If a command is given, run it and exist
        if commands.nonEmpty then
          for line <- commands do
            println(s"flow> ${line}")
            eval(line)
          toContinue = false
        else
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
  private def knownCommands = Set("exit", "quit", "clear", "help")
  private def helpMessage: String =
    """[commands]
      | help   : Show this help message
      | quit   : Exit the REPL
      | exit   : Exit the REPL
      | clear  : Clear the screen
      |""".stripMargin

  /**
    * A custom parser to enable receiving multiline inputs in REPL
    */
  private class ReplParser extends org.jline.reader.Parser:
    private val parser = new DefaultParser()

    override def parse(line: String, cursor: Int, context: ParseContext) =
      val cmd = line.trim
      if cmd.isEmpty || knownCommands.contains(cmd) || context == ParseContext.COMPLETE then
        parser.parse(line, cursor, context)
      else if cmd.endsWith(";") then
        // Finish reading a query
        parser.parse(line, cursor, context)
      else

        def incomplete = throw EOFError(-1, -1, null)

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
              // If statement can be parsed successfully, complete the input
              parser.parse(line, cursor, context)
        catch
          case e: FlowLangException =>
            // Move to the secondary prompt until seeing a semicolon
            incomplete

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
