package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.cli.terminal.HeadlessTerminal
import wvlet.lang.cli.terminal.UniTerminal
import wvlet.lang.runner.FlowExecutionResult
import wvlet.lang.runner.ModelBuildPlan
import wvlet.lang.runner.ModelBuilder
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.PlanScriptRunner
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.cli.TerminalIO

/**
  * The `wvlet` command of the npm and Native distributions: the shared [[WvletCliCommands]],
  * compiled by [[WvletCliCompiler]] and run through the cross-platform `SqlConnectorProvider` (the
  * in-process wvlet engine, DuckDB, and Trino).
  *
  * Each platform (JVM, Node.js, Native) wires its own `main` that reads argv and dispatches through
  * `Launcher.of[WvletCli]`. The JVM CLI's `WvletMain` implements the same commands with the JVM
  * connectors, runs the REPL and builds on them, and adds `ui`, `flow`, and `catalog`.
  */
class WvletCli(opts: WvletCliGlobalOption) extends WvletCliCommands:

  override protected def generateSQL(opt: WvletCliCompileOption): String =
    WvletCliCompiler(opt).generateSQL

  override protected def generateWvlet(opt: WvletCliCompileOption): String =
    WvletCliCompiler(opt).generateWvlet

  override protected def runLocally(opt: WvletCliCompileOption, profile: Profile): QueryResult =
    // Compile with the EFFECTIVE engine's dialect: without this, `-p trino-profile` (no -t)
    // would compile DuckDB SQL and execute it on Trino
    val (unit, ctx) = WvletCliCompiler(opt, profile).compileForRun
    val provider    = SqlConnectorProvider(profile)
    try
      val executor = PlanExecutor(
        provider,
        profile,
        // WARN keeps per-statement "Executing SQL" progress logs out of the CLI output;
        // query results and test outcomes are printed separately
        WorkEnv(opt.workFolder, logLevel = wvlet.uni.log.LogLevel.WARN),
        // The CLI prints every returned row (for piping); no interactive truncation
        rowLimit = Int.MaxValue
      )
      val plan = ExecutionPlanner.plan(unit, ctx)
      executor.execute(plan, ctx)
    finally
      provider.close()

  /**
    * Builds models one at a time on the caller with the cross-platform [[ModelBuilder]]
    */
  override protected def executeBuild(
      opt: WvletBuildOption,
      plan: ModelBuildPlan,
      profile: Profile,
      workEnv: WorkEnv
  ): FlowExecutionResult =
    val builder = ModelBuilder(profile, workEnv)
    try builder.build(plan)
    finally builder.close()

  // Run records are a JVM CLI feature
  override protected def checkBuildOptions(opt: WvletBuildOption): Unit =
    if opt.runStore.isDefined then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException("--run-store needs the JVM wvlet CLI, which records build runs")

  // Without -l or --debug, keep the per-model progress logs out of the output: off the JVM they
  // print to the console instead of a log file
  override protected def buildLogLevel: wvlet.uni.log.LogLevel =
    if opts.debugMode || opts.logLevelName.isDefined then
      opts.logLevel
    else
      wvlet.uni.log.LogLevel.WARN

  /**
    * The REPL over the cross-platform [[PlanScriptRunner]]. On Node.js this returns while the REPL
    * keeps reading inputs, and the REPL closes its resources when it ends
    */
  override protected def startREPL(opt: WvletREPLOption): Unit =
    val scripts       = opt.inputScripts
    val isInteractive = scripts.isEmpty
    // Without -l or --debug, keep the per-statement "Executing ..." logs out of the REPL: off the
    // JVM they print to the console instead of the log file
    val logLevel =
      if opts.debugMode || opts.logLevelName.isDefined then
        opts.logLevel
      else
        wvlet.uni.log.LogLevel.WARN
    val workEnv = WorkEnv(opt.workFolder, logLevel = logLevel)
    val runner  = PlanScriptRunner(
      workEnv,
      opt.resolveProfile,
      interactive = isInteractive,
      // The prompt hides the temp file's name: "(END)" at the end, ":" otherwise
      pager = text => TextCommand.run(text, "less", "-FXRSn", "-Ps?e(END):\\:.")
    )
    val terminal =
      if isInteractive then
        UniTerminal(workEnv, TerminalIO.default)
      else
        HeadlessTerminal()
    val repl = WvletREPL(runner, terminal)
    REPLCompat.runToEnd(repl.start(scripts)) {
      try repl.close()
      finally runner.close()
    }

end WvletCli

object WvletCli:

  /** Commands that only the JVM CLI (`WvletMain`) has */
  val jvmOnlyCommands: Set[String] = Set("ui", "flow", "catalog")

  // The options of WvletCliGlobalOption that take a value, so their value is not taken for the
  // command name. Update together with WvletCliGlobalOption
  private val globalOptionsWithValue = Set("-l", "--loglevel", "-L")

  /**
    * The command name in the given argv: the first argument after the global options
    */
  def commandName(args: Array[String]): Option[String] =
    val it                  = args.iterator
    var cmd: Option[String] = None
    while cmd.isEmpty && it.hasNext do
      val arg = it.next()
      if globalOptionsWithValue.contains(arg) then
        if it.hasNext then
          it.next()
      else if !arg.startsWith("-") then
        cmd = Some(arg)
    cmd

  /**
    * Fail with a message naming the JVM CLI when argv selects one of its commands, instead of the
    * launcher's generic unknown-command error
    */
  def rejectJvmOnlyCommand(args: Array[String]): Unit = commandName(args)
    .filter(jvmOnlyCommands.contains)
    .foreach { cmd =>
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"wvlet ${cmd} is in the JVM CLI. Install it with Homebrew or a release package: " +
            "https://wvlet.org/wvlet/docs/usage/install#homebrew-recommended"
        )
    }

end WvletCli
