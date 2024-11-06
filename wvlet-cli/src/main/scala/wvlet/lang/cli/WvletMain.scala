package wvlet.lang.cli

import wvlet.airframe.launcher.{Launcher, argument, command, option}
import wvlet.lang.BuildInfo
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.server.{WvletServer, WvletServerConfig}
import wvlet.log.LogSupport

object WvletMain:
  private def launcher: Launcher      = Launcher.of[WvletMain]
  def main(args: Array[String]): Unit = launcher.execute(args)
  def main(argLine: String): Unit     = launcher.execute(argLine)

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

  @command(description = "Compile .wv files")
  def compile(compilerOption: WvletCompilerOption): Unit =
    val currentProfile: Profile = Profile
      .getProfile(compilerOption.profile, compilerOption.catalog, compilerOption.schema)

    val compiler = Compiler(
      CompilerOptions(
        phases = Compiler.allPhases,
        workEnv = WorkEnv(compilerOption.workFolder, opts.logLevel),
        catalog = currentProfile.catalog,
        schema = currentProfile.schema
      )
    )

    val inputUnit =
      (compilerOption.file, compilerOption.query) match
        case (Some(f), None) =>
          CompilationUnit.fromFile(s"${compilerOption.workFolder}/${f}".stripPrefix("./"))
        case (None, Some(q)) =>
          CompilationUnit.fromString(q)
        case _ =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("Specify either --file or a query argument")

    val compileResult = compiler.compileSingleUnit(inputUnit)

    compileResult.reportAllErrors



  end compile

end WvletMain

case class WvletCompilerOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None,
    @option(prefix = "--file", description = "Read a query from a file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None
)
