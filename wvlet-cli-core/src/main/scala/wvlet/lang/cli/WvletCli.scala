package wvlet.lang.cli

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.analyzer.duckdb.QueryResultPrinter
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.query.QueryProgressMonitor
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
    val sql    = WvletCliCompiler(opt.toCompileOption).generateSQL
    val result = executeAgainst(sql, opt)
    val output =
      opt.format.toLowerCase match
        case "csv" =>
          QueryResultPrinter.toCsv(result)
        case "box" | "" =>
          QueryResultPrinter.toBox(result)
        case other =>
          throw new IllegalArgumentException(s"Unknown --format: ${other} (supported: box, csv)")
    print(output)

  private def executeAgainst(sql: String, opt: WvletCliRunOption): QueryResult =
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
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val provider               = SqlConnectorProvider()
    try provider.getConnector(config).execute(sql)
    finally provider.close()

  end executeAgainst

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
