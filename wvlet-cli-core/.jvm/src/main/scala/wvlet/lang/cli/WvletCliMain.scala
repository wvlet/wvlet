package wvlet.lang.cli

import wvlet.uni.cli.launcher.Launcher

/**
  * JVM entry for `wvlet-cli-core`. Useful for embedding the cross-platform CLI surface in JVM
  * tooling without dragging in `wvlet-runner` / `wvlet-server`. The full-featured `wvlet` command
  * (with `run` / `ui` / REPL) still ships from `wvlet-cli`.
  */
object WvletCliMain:
  private def launcher: Launcher = Launcher.of[WvletCli]

  def main(args: Array[String]): Unit =
    try
      launcher.execute(args)
    catch
      case e: Throwable =>
        Console.err.println(e.getMessage)
        sys.exit(1)
