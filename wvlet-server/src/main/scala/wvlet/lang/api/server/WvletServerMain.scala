package wvlet.lang.api.server

import wvlet.airframe.http.netty.NettyServer
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.BuildInfo
import wvlet.log.LogSupport

object WvletServerMain:
  private def launcher: Launcher = Launcher.of[WvletServerMain]

  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

class WvletServerMain(
    @option(prefix = "-h,--help", description = "Display help message", isHelp = true)
    displayHelp: Boolean = false
) extends LogSupport:
  @command(description = "Show the version of wvlet-server", isDefault = true)
  def version: Unit = info(s"wvlet server - version: ${BuildInfo.version}")

  @command(description = "Start the wvlet server")
  def start(
      @option(prefix = "-p,--port", description = "Port number to listen")
      port: Int = 8080
  ): Unit =
    val design = WvletServer.design(WvletServerConfig(port = port))
    design.build[NettyServer] { server =>
      server.awaitTermination()
    }
