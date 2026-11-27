package wvlet.lang.cli

import wvlet.uni.cli.launcher.Launcher
import wvlet.uni.cli.launcher.command
import wvlet.uni.design.Design
import wvlet.lang.api.StatusCode.SYNTAX_ERROR
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.cli.terminal.HeadlessTerminal
import wvlet.lang.cli.terminal.UniTerminal
import wvlet.uni.cli.TerminalIO
import wvlet.lang.cli.terminal.REPLTerminal
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.QueryExecutorConfig
import wvlet.lang.runner.WvletScriptRunner
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.activity.ItemStore
import wvlet.lang.runner.activity.ItemStores
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.io.IO
import wvlet.uni.log.LogSupport

import java.io.File

/**
  * The `wv` launcher: an alias of `wvlet repl`, kept for existing scripts and muscle memory
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

@command(usage = "wv [options]", description = "Wvlet REPL (an alias of wvlet repl)")
class WvletREPLMain(cliOption: WvletCliGlobalOption, replOpts: WvletREPLOption) extends LogSupport:

  @command(description = "Start REPL shell", isDefault = true)
  def repl(): Unit =
    val commandInputs = List.newBuilder[String]
    commandInputs ++= replOpts.commands
    replOpts
      .inputFile
      .foreach { file =>
        val f = new File(replOpts.workFolder, file)
        if f.exists() then
          val contents = IO.readString(IO.path(f.getPath))
          commandInputs += contents
        else
          throw StatusCode.FILE_NOT_FOUND.newException(s"File not found: ${f.getAbsolutePath()}")
      }

    val inputScripts = commandInputs.result()
    replDesign(isInteractive = inputScripts.isEmpty).build[WvletREPL] { repl =>
      repl.start(inputScripts)
    }

  end repl

  /**
    * The REPL components. Interactive sessions read from `TerminalIO`, which tests can override
    * with `TerminalIO.inMemory(...)`.
    */
  private[cli] def replDesign(isInteractive: Boolean): Design =
    val currentProfile: Profile =
      replOpts.targetDBType match
        case Some(target) =>
          if replOpts.profile.isDefined then
            throw StatusCode
              .INVALID_ARGUMENT
              .newException("Specify either -t (--target) or --profile")
          Profile
            .defaultProfileFor(DBType.fromString(target))
            .withEngineOverrides(replOpts.catalog, replOpts.schema)
        case None =>
          // Without a profile, the REPL runs on the in-process wvlet engine
          Profile.getProfile(
            replOpts.profile,
            replOpts.catalog,
            replOpts.schema,
            Profile.defaultWvletProfile
          )

    val selectedCatalog = currentProfile.defaultEngine.catalog
    val selectedSchema  = currentProfile.defaultEngine.schema

    // Only interactive sessions touch the native terminal, so that `-c` runs print no JVM
    // native-access warnings
    val terminalDesign =
      if isInteractive then
        Design
          .newSilentDesign
          .bindInstance[TerminalIO](TerminalIO.default)
          .bindProvider[WorkEnv, TerminalIO, REPLTerminal] { (workEnv: WorkEnv, io: TerminalIO) =>
            UniTerminal(workEnv, io)
          }
      else
        Design.newSilentDesign.bindInstance[REPLTerminal](HeadlessTerminal())

    terminalDesign
      .bindProvider[WvletScriptRunner, REPLTerminal, WvletREPL] {
        (runner: WvletScriptRunner, terminal: REPLTerminal) =>
          WvletREPL(runner, terminal)
      }
      .bindInstance[Profile](currentProfile)
      .bindInstance[WorkEnv](WorkEnv(path = replOpts.workFolder, logLevel = cliOption.logLevel))
      // Explicit provider: ConnectorProvider's `factories` parameter has a default the DI layer
      // shouldn't try to resolve
      .bindProvider[WorkEnv, ConnectorProvider] { (workEnv: WorkEnv) =>
        ConnectorProvider(workEnv)
      }
      // Activity items of `run flow` survive the REPL, so a later resume reuses finished items
      .bindProvider[WorkEnv, ItemStore] { (workEnv: WorkEnv) =>
        ItemStores.forWorkEnv(workEnv)
      }
      .onShutdown(_.close())
      .bindProvider[ConnectorProvider, WorkEnv, ItemStore, QueryExecutor] {
        (connectorProvider: ConnectorProvider, workEnv: WorkEnv, itemStore: ItemStore) =>
          QueryExecutor(
            connectorProvider,
            currentProfile,
            workEnv,
            QueryExecutorConfig().withItemStore(itemStore)
          )
      }
      .bindInstance[WvletScriptRunnerConfig](
        WvletScriptRunnerConfig(
          interactive = isInteractive,
          profile = currentProfile,
          catalog = selectedCatalog,
          schema = selectedSchema
        )
      )

  end replDesign

end WvletREPLMain
