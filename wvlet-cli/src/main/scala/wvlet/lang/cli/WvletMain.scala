package wvlet.lang.cli

import wvlet.airframe.launcher.{Launcher, argument, command, option}
import wvlet.lang.BuildInfo
import wvlet.lang.api.v1.query.QuerySelection.All
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.catalog.Profile
import wvlet.lang.cli.WvletMain.isInSbt
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.*
import wvlet.lang.runner.QuerySelector
import wvlet.lang.server.{WvletServer, WvletServerConfig}
import wvlet.log.LogSupport

object WvletMain:
  private def launcher: Launcher      = Launcher.of[WvletMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

  def isInSbt: Boolean = sys.props.getOrElse("wvlet.sbt.testing", "false").toBoolean

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

  private def handleError[U](body: => U): U =
    try
      body
    catch
      case e: WvletLangException =>
        error(e.getMessage)
        if !isInSbt then
          System.exit(1)
        throw e

  @command(description = "Compile .wv files")
  def compile(compilerOption: WvletCompilerOption): Unit = handleError {
    val sql = WvletCompiler(opts, compilerOption).generateSQL
    println(sql)
  }

  @command(description = "Run a query")
  def run(compilerOption: WvletCompilerOption): Unit = handleError {
    WvletCompiler(opts, compilerOption).run()
  }

end WvletMain
