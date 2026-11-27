package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.analyzer.duckdb.QueryResultPrinter as CsvPrinter
import wvlet.lang.compiler.connector.QueryResult as XPQueryResult
import wvlet.lang.compiler.connector.QueryResultRow
import wvlet.lang.runner.PlanResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.QueryResultList
import wvlet.lang.runner.TableRows
import wvlet.lang.runner.connector.WvletServerClient
import wvlet.lang.skill.SkillOption
import wvlet.uni.cli.launcher.command
import wvlet.uni.log.LogSupport

/**
  * The `wvlet` commands shared by every distribution: `version`, `compile`, `to_wvlet`, `skill`,
  * `run`, and `repl`. Each distribution implements only the platform hooks: how to compile, how to
  * run a query locally, and whether it has the REPL. The JVM CLI (`WvletMain`) adds its JVM-only
  * commands on top.
  */
trait WvletCliCommands extends LogSupport:

  /** Compile a query to SQL of the target engine */
  protected def generateSQL(opt: WvletCliCompileOption): String

  /** Convert SQL (or Wvlet) to a Wvlet flow-style query */
  protected def generateWvlet(opt: WvletCliCompileOption): String

  /**
    * Run a query on this machine with the resolved `profile`, which already has the connection
    * flags applied. Remote wvlet servers are handled by [[run]] before this is called.
    */
  protected def runLocally(opt: WvletCliCompileOption, profile: Profile): QueryResult

  /**
    * Start the interactive REPL, or run its `-c`/`-f` commands. Distributions without the REPL keep
    * this default, which names the distribution that has it.
    */
  protected def startREPL(opt: WvletREPLOption): Unit =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException(
        "The REPL is available in the JVM CLI (Homebrew or the release package) for now: https://wvlet.org/wvlet/docs/usage/install"
      )

  /**
    * The profile used when neither `-t` nor `-p` is given: the in-process wvlet engine
    */
  protected def defaultRunProfile: Profile = Profile.defaultWvletProfile

  /**
    * Wrap the body of each command, e.g., to report user errors with an exit code
    */
  protected def handleCommand[U](body: => U): U = body

  @command(description = "show version", isDefault = true)
  def version: Unit = println(s"wvlet ${wvlet.lang.BuildInfo.version}")

  @command(description = "Compile .wv files to SQL")
  def compile(opt: WvletCliCompileOption): Unit = handleCommand(println(generateSQL(opt)))

  @command(description = "Convert SQL to Wvlet flow-style query")
  def to_wvlet(opt: WvletCliCompileOption): Unit = handleCommand(println(generateWvlet(opt)))

  @command(description =
    "Print the AI-agent skill for this wvlet version (SKILL.md, or a reference file), or install it with --install <dir>"
  )
  def skill(opt: SkillOption): Unit = handleCommand(SkillOption.execute(opt))

  @command(description = "Start the interactive REPL (wv is an alias)")
  def repl(opt: WvletREPLOption): Unit = handleCommand(startREPL(opt))

  @command(description =
    "Run a query on the in-process wvlet engine (default), DuckDB, Trino, or a remote wvlet server"
  )
  def run(opt: WvletCliCompileOption, runOpt: WvletCliRunOption): Unit = handleCommand {
    val result = execute(opt, runOpt)
    val output =
      runOpt.format.toLowerCase match
        case "csv" =>
          // CSV covers the final tabular result; test/command outcomes have no tabular shape
          WvletCliCommands.lastTableRows(result).map(WvletCliCommands.toCsv).getOrElse("")
        case "box" | "" =>
          result.toPrettyBox()
        case other =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Unknown --format: ${other} (supported: box, csv)")
    print(output)
    if output.nonEmpty && !output.endsWith("\n") then
      println()
    // Surface test failures and execution errors as a non-zero exit after showing all results
    WvletCliCommands
      .firstError(result)
      .foreach {
        case e: wvlet.lang.api.WvletLangException =>
          throw e
        case e =>
          throw StatusCode.QUERY_EXECUTION_FAILURE.newException(e.getMessage, e)
      }
  }

  /**
    * Run a query: on a remote wvlet server when the effective engine is `wvlet` with a host, and
    * through [[runLocally]] otherwise
    */
  protected def execute(opt: WvletCliCompileOption, runOpt: WvletCliRunOption): QueryResult =
    val profile = runOpt.applyTo(opt.resolveProfile(defaultRunProfile))
    val engine  = profile.defaultEngine
    if engine.`type`.equalsIgnoreCase("wvlet") && engine.host.isDefined then
      // The server compiles and runs the ORIGINAL wvlet text with its own profile, catalogs, and
      // credentials: no local engine or compilation involved
      WvletCliCommands.runOnRemoteServer(opt, engine)
    else
      runLocally(opt, profile)

end WvletCliCommands

object WvletCliCommands:

  /**
    * True for the exceptions that end a command successfully, such as `--version` and `--help`,
    * which the launchers report by throwing
    */
  def isSuccessfulExit(e: Throwable): Boolean =
    e match
      case e: wvlet.lang.api.WvletLangException =>
        e.statusCode.isSuccess
      case e: IllegalArgumentException if e.getCause != null =>
        isSuccessfulExit(e.getCause)
      case _ =>
        false

  /**
    * Send the query text to a wvlet server. Credentials come from the profile only (with ${ENV}
    * interpolation), never from CLI flags.
    */
  def runOnRemoteServer(opt: WvletCliCompileOption, engine: ConnectorConfig): QueryResult =
    val host = engine
      .host
      .getOrElse(throw StatusCode.INVALID_ARGUMENT.newException("--host is required"))
    val useHttps = engine.useHttps.getOrElse(false)
    val port     = engine
      .port
      .getOrElse(
        if useHttps then
          443
        else
          // The wvlet server's default port
          9090
      )
    val scheme =
      if useHttps then
        "https"
      else
        "http"
    val query =
      (opt.file, opt.query) match
        case (Some(f), None) =>
          wvlet.lang.compiler.SourceIO.readAsString(s"${opt.workFolder}/${f}".stripPrefix("./"))
        case (None, Some(q)) =>
          q
        case _ =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("Specify either --file or a query argument")
    val client = WvletServerClient(
      baseUri = s"${scheme}://${host}:${port}",
      // The server resolves this name in ITS profiles.json; unset runs on the server default
      profile = engine.properties.get("remoteProfile").map(_.toString),
      token = engine.properties.get("token").map(_.toString)
    )
    try client.runQuery(query)
    finally client.close()

  end runOnRemoteServer

  /**
    * The first error in a (possibly nested) query result. `PlanResult.getError` doesn't look into
    * its result, so a test failure under a plan would otherwise exit successfully
    */
  def firstError(r: QueryResult): Option[Throwable] =
    r match
      case l: QueryResultList =>
        l.list.iterator.flatMap(firstError).nextOption()
      case p: PlanResult =>
        firstError(p.result)
      case other =>
        other.getError

  /** The last table in a (possibly nested) query result */
  def lastTableRows(r: QueryResult): Option[TableRows] =
    r match
      case t: TableRows =>
        Some(t)
      case l: QueryResultList =>
        l.list.reverseIterator.flatMap(x => lastTableRows(x)).nextOption()
      case p: PlanResult =>
        lastTableRows(p.result)
      case _ =>
        None

  /** Render a table as CSV with a header line */
  def toCsv(t: TableRows): String =
    val columns = t.schema.fields.toList
    val rows    =
      t.rows
        .map { row =>
          QueryResultRow(columns.map(c => Option(row.getOrElse(c.name.name, null)).map(_.toString)))
        }
        .toList
    CsvPrinter.toCsv(XPQueryResult(columns, rows))

end WvletCliCommands
