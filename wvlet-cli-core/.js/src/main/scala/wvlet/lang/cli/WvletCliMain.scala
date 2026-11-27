package wvlet.lang.cli

import wvlet.uni.cli.TerminalIO
import wvlet.uni.cli.launcher.Launcher

import scala.scalajs.js

/**
  * Node.js entry for the wvlet CLI. Linked into a single ESModule JS file by Scala.js with
  * `scalaJSUseMainModuleInitializer := true`. Run via:
  * {{{
  *   node wvlet-cli-core/.js/target/scala-3.x/wvlet-cli-core-fastopt/main.js compile -f foo.wv
  * }}}
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
    // Scala.js's main initializer doesn't wire `process.argv` into `args` automatically — when
    // `scalaJSUseMainModuleInitializer` is true the linker calls `main(Array.empty)`. Pull the
    // real argv off `process.argv` (slicing past `[node, script]`).
    val argv     = js.Dynamic.global.process.argv.asInstanceOf[js.Array[String]]
    val realArgs = argv.toArray.drop(2)
    try
      launcher.execute(WvletCliMain.withDefaultCommand(realArgs))
    catch
      case e: Throwable if WvletCliCommands.isSuccessfulExit(e) =>
      // --version: the version is already printed
      case e: Throwable =>
        Console.err.println(e.getMessage)
        js.Dynamic.global.process.exit(1)
