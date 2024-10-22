package wvlet.lang.server

import wvlet.airframe.http.netty.NettyServer
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.compiler.WorkEnv
import wvlet.log.{LogLevel, LogSupport}

object WvletServerMain:
  private def launcher: Launcher      = Launcher.of[WvletServerMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletServerMain(serverConfig: WvletServerConfig) extends LogSupport:
  @command(description = "Start a local WebUI server", isDefault = true)
  def server(): Unit = WvletServer.startServer(serverConfig, openBrowser = false)
