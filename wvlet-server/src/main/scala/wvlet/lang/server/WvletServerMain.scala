package wvlet.lang.server

import wvlet.airframe.http.netty.NettyServer
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.cli.{WvletGlobalOption, WvletREPLMain, WvletREPLOption}
import wvlet.log.{LogLevel, LogSupport}

object WvletServerMain:
  private def launcher: Launcher      = Launcher.of[WvletServerMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletServerMain(opts: WvletGlobalOption) extends LogSupport:
  @command(description = "Show the version", isDefault = true)
  def version: Unit = info(opts.versionString)

  @command(description = "Start a Wvlet REPL shell")
  def shell(replOpts: WvletREPLOption): Unit = WvletREPLMain(opts, replOpts).repl()

  @command(description = "Start a local WebUI server")
  def ui(serverConfig: WvletServerConfig): Unit = WvletServer.startServer(serverConfig)
