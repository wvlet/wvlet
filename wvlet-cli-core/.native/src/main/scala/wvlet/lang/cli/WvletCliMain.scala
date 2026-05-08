package wvlet.lang.cli

import wvlet.uni.cli.launcher.Launcher

/**
  * Native entry for the wvlet CLI. Built into a standalone executable by Scala Native. Same command
  * surface (`version` / `compile` / `to_wvlet`) as the JVM and Node entries.
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
