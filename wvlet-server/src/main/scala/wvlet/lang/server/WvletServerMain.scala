package wvlet.lang.server

import wvlet.lang.compiler.WorkEnv
import wvlet.uni.cli.launcher.Launcher
import wvlet.uni.cli.launcher.command
import wvlet.uni.log.LogLevel
import wvlet.uni.log.LogSupport

object WvletServerMain:
  private def launcher: Launcher      = Launcher.of[WvletServerMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletServerMain(serverConfig: WvletServerConfig) extends LogSupport:
  @command(description = "Start the wvlet server and its web UI", isDefault = true)
  def server(): Unit = WvletServer.startServer(serverConfig)
