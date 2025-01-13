package wvlet.lang.cli

import wvlet.airframe.Design
import wvlet.airframe.launcher.{Launcher, command}
import wvlet.lang.BuildInfo
import wvlet.lang.api.WvletLangException
import wvlet.lang.cli.WvletMain.isInSbt
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.lang.server.{WvletServer, WvletServerConfig}
import wvlet.log.LogSupport

object WvletMain:
  private def launcher: Launcher = Launcher.of[WvletMain]

  private def wrap(body: => Unit): Unit =
    def findCause(e: Throwable): Throwable =
      e match
        case e: IllegalArgumentException if e.getCause != null =>
          findCause(e.getCause)
        case other =>
          e

    try
      body
    catch
      case e: Throwable =>
        findCause(e) match
          case e: WvletLangException if e.statusCode.isSuccess =>
          // do nothing
          case other =>
            throw other

  def main(args: Array[String]): Unit = wrap(launcher.execute(args))
  def main(argLine: String): Unit     = wrap(launcher.execute(argLine))

  def isInSbt: Boolean = sys.props.getOrElse("wvlet.sbt.testing", "false").toBoolean

/**
  * 'wvlet' command line interface
  * @param opts
  */
class WvletMain(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "show version", isDefault = true)
  def version: Unit = info(s"wvlet version ${BuildInfo.version}")

  @command(description = "Start a local WebUI server")
  def ui(serverConfig: WvletServerConfig): Unit = WvletServer.startServer(
    serverConfig,
    openBrowser = true
  )

  private def handleError[U](body: => U): U =
    try
      body
    catch
      case e: WvletLangException =>
        error(e.getMessage)
        if !isInSbt then
          System.exit(1)
        throw e

  private def design(compilerOptions: WvletCompilerOption): Design =
    val workEnv = WorkEnv(compilerOptions.workFolder, opts.logLevel)
    Design
      .newSilentDesign
      .bindInstance(WvletCompiler(opts, compilerOptions, workEnv, DBConnectorProvider(workEnv)))

  @command(description = "Compile .wv files")
  def compile(compilerOption: WvletCompilerOption): Unit = handleError {
    design(compilerOption).build[WvletCompiler] { compiler =>
      val sql = compiler.generateSQL
      println(sql)
    }
  }

  @command(description = "Run a query")
  def run(compilerOption: WvletCompilerOption): Unit = handleError {
    design(compilerOption).build[WvletCompiler] { compiler =>
      compiler.run()
    }
  }

end WvletMain
