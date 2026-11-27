package wvlet.lang.cli

import wvlet.uni.cli.TerminalIO
import wvlet.uni.cli.launcher.Launcher

/**
  * Native entry for the wvlet CLI. Built into a standalone executable by Scala Native. Same command
  * surface ([[WvletCliCommands]]) as the JVM and Node entries.
  */
object WvletCliMain:

  /**
    * `wvlet` with no arguments on an interactive terminal opens the REPL, like `duckdb` or
    * `python`. Pipes, CI, and agents get the version instead, so a bare `wvlet` never waits for
    * input there.
    */
  def withDefaultCommand(args: Array[String]): Array[String] =
    if args.isEmpty && TerminalIO.default.isTTY then
      Array("repl")
    else
      args

  private def launcher: Launcher = Launcher.of[WvletCli]

  def main(args: Array[String]): Unit =
    try
      launcher.execute(WvletCliMain.withDefaultCommand(args))
    catch
      case e: Throwable if WvletCliCommands.isSuccessfulExit(e) =>
      // --version: the version is already printed
      case e: Throwable =>
        Console.err.println(e.getMessage)
        sys.exit(1)
