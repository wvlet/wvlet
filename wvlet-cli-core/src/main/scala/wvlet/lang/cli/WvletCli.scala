package wvlet.lang.cli

import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.compiler.analyzer.duckdb.QueryResultPrinter
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

  @command(description = "Compile and execute a wvlet query against DuckDB")
  def run(opt: WvletCliRunOption): Unit =
    if !DuckDB.canExecute then
      throw new UnsupportedOperationException(
        "DuckDB execution is not available on this platform. " +
          "Ensure libduckdb is installed and discoverable, or set WVLET_LIBDUCKDB."
      )
    val sql    = WvletCliCompiler(opt.toCompileOption).generateSQL
    val result = DuckDB.execute(sql)
    val output =
      opt.format.toLowerCase match
        case "csv" =>
          QueryResultPrinter.toCsv(result)
        case "box" | "" =>
          QueryResultPrinter.toBox(result)
        case other =>
          throw new IllegalArgumentException(s"Unknown --format: ${other} (supported: box, csv)")
    print(output)

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
    @option(prefix = "-t,--target", description = "Target database type (always duckdb for `run`)")
    targetDBType: Option[String] = None,
    @option(prefix = "--format", description = "Output format: box (default), csv")
    format: String = "box"
):
  def toCompileOption: WvletCliCompileOption = WvletCliCompileOption(
    workFolder = workFolder,
    file = file,
    query = query,
    targetDBType = targetDBType
  )
