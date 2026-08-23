package wvlet.lang.cli

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.analyzer.duckdb.QueryResultPrinter as CsvPrinter
import wvlet.lang.compiler.connector.QueryResult as XPQueryResult
import wvlet.lang.compiler.connector.QueryResultRow
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.PlanResult
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.QueryResultList
import wvlet.lang.runner.TableRows
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.command
import wvlet.uni.cli.launcher.option
import wvlet.uni.log.LogSupport

/**
  * Cross-platform wvlet CLI surface — `version`, `compile`, `to_wvlet`, `run`.
  *
  * Each platform (JVM, Node.js, Native) wires its own `main` that reads argv and dispatches through
  * `Launcher.of[WvletCli]`. The JVM `wvlet-cli` module additionally exposes `ui` and the REPL via
  * its own entry point.
  */
class WvletCli(opts: WvletCliGlobalOption) extends LogSupport:

  @command(description = "show version", isDefault = true)
  def version: Unit = println(s"wvlet ${wvlet.lang.BuildInfo.version}")

  @command(description = "Compile .wv files to SQL")
  def compile(opt: WvletCliCompileOption): Unit = println(WvletCliCompiler(opt).generateSQL)

  @command(description = "Convert SQL to wvlet flow-style query")
  def to_wvlet(opt: WvletCliCompileOption): Unit = println(WvletCliCompiler(opt).generateWvlet)

  @command(description = "Compile and execute a wvlet query against DuckDB or Trino")
  def run(opt: WvletCliRunOption): Unit =
    val result = executeAgainst(opt)
    val output =
      opt.format.toLowerCase match
        case "csv" =>
          // CSV covers the final tabular result; test/command outcomes have no tabular shape
          lastTableRows(result).map(t => CsvPrinter.toCsv(toXPResult(t))).getOrElse("")
        case "box" | "" =>
          result.toPrettyBox()
        case other =>
          throw new IllegalArgumentException(s"Unknown --format: ${other} (supported: box, csv)")
    print(output)
    // Surface test failures and execution errors as a non-zero exit after showing all results
    result.getError.foreach(e => throw e)

  private def executeAgainst(opt: WvletCliRunOption): QueryResult =
    val profile = opt.profile.flatMap(Profile.getProfile)
    val engine  = profile.map(_.defaultEngine)
    // Effective backend: --target wins, then the profile's default engine type, then "duckdb".
    val backend = opt
      .targetDBType
      .orElse(engine.map(_.`type`))
      .map(_.toLowerCase)
      .getOrElse("duckdb")
    // CLI flags override the matching profile field. `--https` (a Boolean flag) can't
    // distinguish "user passed false" from "user didn't pass it", so the profile setting only
    // applies when the flag was left at the default `false`.
    val config = ConnectorConfig(
      name = backend,
      `type` = backend,
      default = true,
      user = opt.user.orElse(engine.flatMap(_.user)),
      // Credentials come from the profile only (with ${ENV} interpolation) — never CLI flags
      password = engine.flatMap(_.password),
      properties = engine.map(_.properties).getOrElse(Map.empty),
      host = opt.host.orElse(engine.flatMap(_.host)),
      port = opt.port.orElse(engine.flatMap(_.port)),
      catalog = opt.catalog.orElse(engine.flatMap(_.catalog)),
      schema = opt.schema.orElse(engine.flatMap(_.schema)),
      useHttps = Some(
        if opt.useHttps then
          true
        else
          engine.flatMap(_.useHttps).getOrElse(false)
      )
    )
    // The effective profile: the merged config is the default engine; the other profile
    // connectors stay addressable via `use <name>`
    val runProfile = Profile(
      name = profile.map(_.name).getOrElse("default"),
      connectors =
        config +:
          profile
            .map(_.connectors.filterNot(_.name == config.name).map(_.withDefault(false)))
            .getOrElse(Nil)
    )
    val (unit, ctx) = WvletCliCompiler(opt.toCompileOption).compileForRun
    val provider    = SqlConnectorProvider(runProfile)
    try
      val executor = PlanExecutor(
        provider,
        runProfile,
        // WARN keeps per-statement "Executing SQL" progress logs out of the CLI output;
        // query results and test outcomes are printed separately below
        WorkEnv(opt.workFolder, logLevel = wvlet.uni.log.LogLevel.WARN),
        // The CLI prints every returned row (for piping); no interactive truncation
        rowLimit = Int.MaxValue
      )
      val plan = ExecutionPlanner.plan(unit, ctx)
      executor.execute(plan, ctx)
    finally
      provider.close()

  end executeAgainst

  private def lastTableRows(r: QueryResult): Option[TableRows] =
    r match
      case t: TableRows =>
        Some(t)
      case l: QueryResultList =>
        l.list.reverseIterator.flatMap(x => lastTableRows(x)).nextOption()
      case p: PlanResult =>
        lastTableRows(p.result)
      case _ =>
        None

  private def toXPResult(t: TableRows): XPQueryResult =
    val columns = t.schema.fields.toList
    val rows    =
      t.rows
        .map { row =>
          QueryResultRow(columns.map(c => Option(row.getOrElse(c.name.name, null)).map(_.toString)))
        }
        .toList
    XPQueryResult(columns, rows)

end WvletCli

case class WvletCliGlobalOption(
    @option(prefix = "-l,--loglevel", description = "Log level (trace, debug, info, warn, error)")
    logLevel: Option[String] = None
)

case class WvletCliCompileOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "-f,--file", description = "Read a query from the given file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None,
    @option(prefix = "-t,--target", description = "Target database type (duckdb, trino, ...)")
    targetDBType: Option[String] = None
)

case class WvletCliRunOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "-f,--file", description = "Read a query from the given file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None,
    @option(prefix = "-p,--profile", description = "Profile name from ~/.wvlet/profiles.json")
    profile: Option[String] = None,
    @option(prefix = "-t,--target", description = "Backend: duckdb (default) or trino")
    targetDBType: Option[String] = None,
    @option(prefix = "--format", description = "Output format: box (default), csv")
    format: String = "box",
    @option(prefix = "--host", description = "Trino coordinator host (overrides profile)")
    host: Option[String] = None,
    @option(
      prefix = "--port",
      description = "Trino coordinator port (default 443 with --https, else 8080)"
    )
    port: Option[Int] = None,
    @option(prefix = "--user", description = "Trino user (default 'wvlet')")
    user: Option[String] = None,
    @option(prefix = "--catalog", description = "Trino catalog")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Trino schema")
    schema: Option[String] = None,
    @option(prefix = "--https", description = "Use HTTPS to reach the Trino coordinator")
    useHttps: Boolean = false
):
  def toCompileOption: WvletCliCompileOption = WvletCliCompileOption(
    workFolder = workFolder,
    file = file,
    query = query,
    targetDBType = targetDBType
  )

end WvletCliRunOption
