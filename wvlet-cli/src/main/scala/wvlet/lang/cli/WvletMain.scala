package wvlet.lang.cli

import wvlet.airframe.http.netty.NettyServer
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.api.server.{WvletServer, WvletServerConfig}
import wvlet.log.{LogLevel, LogSupport}

object WvletMain:
  private def launcher: Launcher      = Launcher.of[WvletMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletMain(opts: WvletGlobalOption) extends LogSupport:
  @command(description = "Show the version", isDefault = true)
  def version: Unit = info(opts.versionString)

  @command(description = "Start a local WebUI server")
  def ui(
      @option(prefix = "-p,--port", description = "Port number to listen")
      port: Int = 9090
  ): Unit =
    val design = WvletServer.design(WvletServerConfig(port = port))
    design.build[NettyServer] { server =>
      info(s"Press ctrl+c to stop the server")
      server.awaitTermination()
    }
