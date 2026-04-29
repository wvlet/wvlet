package wvlet.lang.cli

import wvlet.lang.BuildInfo
import wvlet.lang.api.StatusCode
import wvlet.uni.cli.launcher.option
import wvlet.uni.log.LogLevel
import wvlet.uni.log.LogSupport
import wvlet.uni.log.Logger

case class WvletGlobalOption(
    @option(prefix = "--version", description = "Display the version")
    displayVersion: Boolean = false,
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

  if displayVersion then
    info(versionString)
    throw StatusCode.EXIT_SUCCESSFULLY.newException("exit successfully")

  logLevelPatterns.foreach { p =>
    p.split("=") match
      case Array(pattern, level) =>
        debug(s"Set the log level for ${pattern} to ${level}")
        Logger.setLogLevel(pattern, LogLevel(level))
      case _ =>
        error(s"Invalid log level pattern: ${p}")
  }

end WvletGlobalOption
