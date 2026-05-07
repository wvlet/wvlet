package wvlet.lang.cli

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
  private def launcher: Launcher = Launcher.of[WvletCli]

  def main(args: Array[String]): Unit =
    // Scala.js's main initializer doesn't wire `process.argv` into `args` automatically — when
    // `scalaJSUseMainModuleInitializer` is true the linker calls `main(Array.empty)`. Pull the
    // real argv off `process.argv` (slicing past `[node, script]`).
    val argv     = js.Dynamic.global.process.argv.asInstanceOf[js.Array[String]]
    val realArgs = argv.toArray.drop(2)
    try
      launcher.execute(realArgs)
    catch
      case e: Throwable =>
        Console.err.println(e.getMessage)
        js.Dynamic.global.process.exit(1)
