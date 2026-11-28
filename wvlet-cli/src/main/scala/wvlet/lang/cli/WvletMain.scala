package wvlet.lang.cli

import wvlet.uni.cli.launcher.Launcher
import wvlet.uni.cli.launcher.command
import wvlet.uni.design.Design
import wvlet.uni.log.LogLevel
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.cli.WvletMain.isInSbt
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.FlowExecutionResult
import wvlet.lang.runner.ModelBuildPlan
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.server.WvletServer
import wvlet.lang.server.WvletServerConfig

object WvletMain:
  private def launcher: Launcher = Launcher
    .of[WvletMain]
    .addModule[WvletFlowCommand]("flow", "Manage and run flows")
    .addModule[WvletCatalogCommand](
      "catalog",
      "Import database table schemas as Wvlet type definitions"
    )

  private def wrap(body: => Unit): Unit =
    try
      body
    catch
      case e: Throwable if WvletCliCommands.isSuccessfulExit(e) =>
      // --version or --help: nothing more to report
      case e: IllegalArgumentException if e.getCause != null =>
        throw e.getCause

  /**
    * `wvlet` with no arguments on an interactive terminal opens the REPL, like `duckdb` or
    * `python`. Pipes, CI, and agents get the version instead, so a bare `wvlet` never waits for
    * input there.
    */
  def main(args: Array[String]): Unit =
    if args.isEmpty && isInteractiveTerminal then
      WvletREPLMain.main(args)
    else
      wrap(launcher.execute(args))

  def main(argLine: String): Unit = wrap(launcher.execute(argLine))

  def isInSbt: Boolean = sys.props.getOrElse("wvlet.sbt.testing", "false").toBoolean

  /**
    * True when stdin and stdout are a terminal. On JDK 22+, `System.console()` also exists for
    * redirected streams, so `Console.isTerminal` (JDK 22+) decides there
    */
  def isInteractiveTerminal: Boolean = Option(System.console()).exists { console =>
    try
      console.getClass.getMethod("isTerminal").invoke(console) == true
    catch
      case _: NoSuchMethodException =>
        // Before JDK 22, a console exists only for a terminal
        true
  }

end WvletMain

/**
  * The JVM `wvlet` command: the commands shared with the npm and Native distributions
  * ([[WvletCliCommands]]), run with the JVM connectors (`build` runs on the flow executor), plus
  * the JVM-only `server`, `flow`, and `catalog` commands
  */
class WvletMain(opts: WvletCliGlobalOption) extends WvletCliCommands:

  @command(description = "Start the wvlet server and its web UI")
  def server(serverConfig: WvletServerConfig): Unit = WvletServer.startServer(serverConfig)

  private def handleError[U](body: => U): U =
    try
      body
    catch
      case e: WvletLangException if !e.statusCode.isSuccess =>
        error(e.getMessage)
        if !isInSbt then
          System.exit(1)
        throw e

  /** Run `f` with a compiler for the options and the effective profile */
  private def withCompiler[A](compilerOption: WvletCliCompileOption, profile: Profile)(
      f: WvletCompiler => A
  ): A =
    val workEnv = WorkEnv(compilerOption.workFolder, opts.logLevel)
    Design
      .newSilentDesign
      .bindInstance(
        WvletCompiler(opts, compilerOption, workEnv, ConnectorProvider(workEnv), profile)
      )
      .run[WvletCompiler, A](f)

  // Compiling to SQL defaults to the DuckDB dialect (catalog `memory`)
  private def compileProfile(opt: WvletCliCompileOption): Profile = opt.resolveProfile(
    Profile.defaultDuckDBProfile
  )

  override protected def generateSQL(opt: WvletCliCompileOption): String =
    withCompiler(opt, compileProfile(opt))(_.generateSQL)

  override protected def generateWvlet(opt: WvletCliCompileOption): String =
    withCompiler(opt, compileProfile(opt))(_.generateWvlet)

  override protected def runLocally(opt: WvletCliCompileOption, profile: Profile): QueryResult =
    withCompiler(opt, profile)(_.execute())

  override protected def startREPL(opt: WvletREPLOption): Unit = WvletREPLMain(opts, opt).repl()

  override protected def handleCommand[U](body: => U): U = handleError(body)

  override protected def executeBuild(
      opt: WvletBuildOption,
      plan: ModelBuildPlan,
      profile: Profile,
      workEnv: WorkEnv
  ): FlowExecutionResult = WvletFlowCommand(opts).executeBuild(opt, plan, profile, workEnv)

  override protected def buildLogLevel: LogLevel = opts.logLevel

end WvletMain
