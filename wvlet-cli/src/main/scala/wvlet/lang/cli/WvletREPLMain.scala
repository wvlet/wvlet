package wvlet.lang.cli

import wvlet.airframe.Design
import wvlet.airframe.launcher.Launcher
import wvlet.airframe.launcher.argument
import wvlet.airframe.launcher.command
import wvlet.airframe.launcher.option
import wvlet.lang.api.StatusCode.SYNTAX_ERROR
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File

/**
  * REPL command launcher (wv)
  */
object WvletREPLMain extends LogSupport:
  def launcher = Launcher.of[WvletREPLMain]

  private def wrap(body: => Unit): Unit =
    try
      body
    catch
      case e: IllegalArgumentException if e.getMessage.contains("exit successfully") =>
      // Exit successfully
      case e: WvletLangException if e.statusCode == StatusCode.EXIT_SUCCESSFULLY =>
      // Exit successfully
      case e: WvletLangException if e.statusCode.isUserError =>
        error(e.getMessage())
      case other =>
        error(other)

  def main(args: Array[String]): Unit = wrap(launcher.execute(args))
  def main(argLine: String): Unit     = wrap(launcher.execute(argLine))

case class WvletREPLOption(
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "-c", description = "Run a command and exit")
    commands: List[String] = Nil,
    @option(prefix = "-f,--file", description = "Run commands in a file and exit")
    inputFile: Option[String] = None,
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
)

@command(usage = "wv [options]", description = "Wvlet REPL")
class WvletREPLMain(cliOption: WvletGlobalOption, replOpts: WvletREPLOption) extends LogSupport:

  @command(description = "Start REPL shell", isDefault = true)
  def repl(): Unit =
    val currentProfile: Profile = Profile.getProfile(
      replOpts.profile,
      replOpts.catalog,
      replOpts.schema,
      Profile.defaultDuckDBProfile
    )

    val selectedCatalog = currentProfile.catalog
    val selectedSchema  = currentProfile.schema

    val commandInputs = List.newBuilder[String]
    commandInputs ++= replOpts.commands
    replOpts
      .inputFile
      .foreach { file =>
        val f = new File(replOpts.workFolder, file)
        if f.exists() then
          val contents = IOUtil.readAsString(f)
          commandInputs += contents
        else
          throw StatusCode.FILE_NOT_FOUND.newException(s"File not found: ${f.getAbsolutePath()}")
      }

    val inputScripts  = commandInputs.result()
    val isInteractive = inputScripts.isEmpty

    val design = Design
      .newSilentDesign
      .bind[WvletREPL]
      .toProvider { (workEnv: WorkEnv, runner: WvletScriptRunner) =>
        WvletREPL(workEnv, runner, isInteractive)
      }
      .bindInstance[Profile](currentProfile)
      .bindInstance[WorkEnv](WorkEnv(path = replOpts.workFolder, logLevel = cliOption.logLevel))
      .bindInstance[WvletScriptRunnerConfig](
        WvletScriptRunnerConfig(
          interactive = isInteractive,
          profile = currentProfile,
          catalog = selectedCatalog,
          schema = selectedSchema
        )
      )

    design.build[WvletREPL] { repl =>
      repl.start(inputScripts)
    }

  end repl

end WvletREPLMain
