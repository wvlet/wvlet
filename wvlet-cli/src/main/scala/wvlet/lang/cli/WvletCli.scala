package wvlet.lang.cli

import wvlet.airframe.http.netty.NettyServer
import wvlet.airframe.launcher.{Launcher, argument, command, option}
import wvlet.lang.BuildInfo
import wvlet.lang.api.server.{WvletServer, WvletServerConfig}
import wvlet.log.{LogLevel, LogSupport, Logger}

object WvletCli:
  private def launcher: Launcher      = Launcher.of[WvletCli]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

case class WvletCliOption(
    @option(prefix = "-h,--help", description = "Display help message", isHelp = true)
    displayHelp: Boolean = false,
    @option(prefix = "--debug", description = "Enable debug log")
    debugMode: Boolean = false,
    @option(prefix = "-l", description = "log level")
    logLevel: LogLevel = LogLevel.INFO,
    @option(prefix = "-L", description = "log level for a class pattern")
    logLevelPatterns: List[String] = List.empty
) extends LogSupport:
  Logger("wvlet.lang.runner").setLogLevel {
    if debugMode then
      LogLevel.DEBUG
    else
      logLevel
  }

  def versionString = s"wvlet version: ${BuildInfo.version} (Built at: ${BuildInfo.builtAtString})"
  debug(versionString)

  logLevelPatterns.foreach { p =>
    p.split("=") match
      case Array(pattern, level) =>
        debug(s"Set the log level for ${pattern} to ${level}")
        Logger.setLogLevel(pattern, LogLevel(level))
      case _ =>
        error(s"Invalid log level pattern: ${p}")
  }

end WvletCliOption

class WvletCli(opts: WvletCliOption) extends LogSupport:
  @command(description = "Show the version")
  def version: Unit = info(opts.versionString)

  @command(description = "Start REPL shell", isDefault = true)
  def repl(
      @argument(description = "repl arguments")
      args: Seq[String] = Seq.empty
  ): Unit = warn(s"REPL args: ${args}")
  // WvletREPL.startREPL(opts, replOpts)

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
