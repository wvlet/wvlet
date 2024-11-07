package wvlet.lang.cli

import wvlet.airframe.Design
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.cli.WvletREPL.{debug, error}
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File

/**
  * REPL command launcher (wv)
  */
object WvletREPLMain:
  def launcher = Launcher.of[WvletREPLMain]

  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

case class WvletREPLOption(
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "-c", description = "Run a command and exit")
    commands: List[String] = Nil,
    @option(prefix = "--file", description = "Run commands in a file and exit")
    inputFile: Option[String] = None,
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
)

class WvletREPLMain(cliOption: WvletGlobalOption, replOpts: WvletREPLOption) extends LogSupport:

  @command(description = "Start REPL shell", isDefault = true)
  def repl(): Unit =
    val currentProfile: Profile = Profile
      .getProfile(replOpts.profile, replOpts.catalog, replOpts.schema, Profile.defaultDuckDBProfile)

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

    val inputScripts = commandInputs.result()

    val design = Design
      .newSilentDesign
      .bindSingleton[WvletREPL]
      .bindInstance[WorkEnv](WorkEnv(path = replOpts.workFolder, logLevel = cliOption.logLevel))
      .bindInstance[WvletScriptRunnerConfig](
        WvletScriptRunnerConfig(
          interactive = inputScripts.isEmpty,
          catalog = selectedCatalog,
          schema = selectedSchema
        )
      )
      .bindInstance[DBConnector](DBConnectorProvider.getConnector(currentProfile))

    design.build[WvletREPL] { repl =>
      repl.start(inputScripts)
    }

  end repl

end WvletREPLMain
