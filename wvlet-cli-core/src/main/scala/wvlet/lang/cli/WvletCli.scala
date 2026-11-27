package wvlet.lang.cli

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.connector.SqlConnectorProvider

/**
  * The `wvlet` command of the npm and Native distributions: the shared [[WvletCliCommands]],
  * compiled by [[WvletCliCompiler]] and run through the cross-platform `SqlConnectorProvider` (the
  * in-process wvlet engine, DuckDB, and Trino).
  *
  * Each platform (JVM, Node.js, Native) wires its own `main` that reads argv and dispatches through
  * `Launcher.of[WvletCli]`. The JVM CLI's `WvletMain` implements the same commands with the JVM
  * connectors and adds the REPL, `ui`, `build`, `flow`, and `catalog`.
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

end WvletCli
