package wvlet.lang.cli

import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.compiler.analyzer.duckdb.QueryResultPrinter
import wvlet.lang.compiler.analyzer.trino.Trino
import wvlet.lang.compiler.analyzer.trino.TrinoConfig
import wvlet.lang.compiler.connector.QueryResult
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
    opt.targetDBType.map(_.toLowerCase).getOrElse("duckdb") match
      case "duckdb" =>
        if !DuckDB.canExecute then
          throw new UnsupportedOperationException(
            "DuckDB execution is not available on this platform. " +
              "Ensure libduckdb is installed and discoverable, or set WVLET_LIBDUCKDB."
          )
        DuckDB.execute(sql)
      case "trino" =>
        val host = opt
          .host
          .getOrElse(throw new IllegalArgumentException("--host is required for --target=trino"))
        val cfg = TrinoConfig(
          host = host,
          port = opt.port.getOrElse(8080),
          user = opt.user.getOrElse("wvlet"),
          catalog = opt.catalog,
          schema = opt.schema,
          useHttps = opt.useHttps
        )
        Trino.execute(sql, cfg)
      case other =>
        throw new IllegalArgumentException(s"Unsupported --target for `run`: ${other}")

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
    @option(prefix = "-t,--target", description = "Backend: duckdb (default) or trino")
    targetDBType: Option[String] = None,
    @option(prefix = "--format", description = "Output format: box (default), csv")
    format: String = "box",
    @option(prefix = "--host", description = "Trino coordinator host (target=trino only)")
    host: Option[String] = None,
    @option(prefix = "--port", description = "Trino coordinator port (default 8080)")
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
