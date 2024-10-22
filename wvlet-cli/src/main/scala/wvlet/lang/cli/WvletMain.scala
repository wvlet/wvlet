package wvlet.lang.cli

import wvlet.airframe.launcher.{Launcher, command}
import wvlet.lang.BuildInfo
import wvlet.lang.server.{WvletServer, WvletServerConfig}
import wvlet.log.LogSupport

object WvletMain:
  private def launcher: Launcher      = Launcher.of[WvletMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletMain(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "show version", isDefault = true)
  def version: Unit = info(s"wvlet version ${BuildInfo.version}")

  @command(description = "Start a local WebUI server")
  def ui(serverConfig: WvletServerConfig): Unit = WvletServer
    .startServer(serverConfig, openBrowser = true)
