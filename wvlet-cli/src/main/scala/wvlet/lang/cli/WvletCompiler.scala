package wvlet.lang.cli

import wvlet.airframe.control.Control
import wvlet.airframe.launcher.{argument, option}
import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.api.v1.query.QuerySelection.All
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{
  CompilationUnit,
  CompileResult,
  Compiler,
  CompilerOptions,
  Context,
  DBType,
  Symbol,
  WorkEnv
}
import wvlet.lang.runner.{QueryExecutor, WvletScriptRunner}
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.log.LogSupport

case class WvletCompilerOption(
    @option(prefix = "-w", description = "Working folder")
    workFolder: String = ".",
    @option(prefix = "-f,--file", description = "Read a query from the given .wv file")
    file: Option[String] = None,
    @argument(description = "query")
    query: Option[String] = None,
    @option(prefix = "-t,--target", description = "Target database type")
    targetDBType: Option[String] = None,
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
)

class WvletCompiler(opts: WvletGlobalOption, compilerOption: WvletCompilerOption)
    extends LogSupport:

  private lazy val currentProfile: Profile =
    compilerOption.targetDBType match
      case Some(dbType) =>
        if compilerOption.profile.isDefined then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("Specify either -t (--target) or --profile")
        val resolvedDBType = DBType.fromString(dbType)
        debug(s"Using syntax for ${resolvedDBType}")
        Profile(name = "local", `type` = resolvedDBType.toString)
      case _ =>
        Profile.getProfile(compilerOption.profile, compilerOption.catalog, compilerOption.schema)

  private lazy val dbConnector: DBConnector = DBConnectorProvider.getConnector(currentProfile)

  private val compiler: Compiler =
    val compiler = Compiler(
      CompilerOptions(
        phases = Compiler.allPhases,
        sourceFolders = List(compilerOption.workFolder),
        workEnv = WorkEnv(compilerOption.workFolder, opts.logLevel),
        catalog = currentProfile.catalog,
        schema = currentProfile.schema
      )
    )
    currentProfile
      .catalog
      .foreach { catalog =>
        val schema = currentProfile.schema.getOrElse("main")
        compiler.setDefaultCatalog(dbConnector.getCatalog(catalog, schema))
      }

    currentProfile
      .schema
      .foreach { schema =>
        compiler.setDefaultSchema(schema)
      }

    compiler

  private lazy val inputUnit: CompilationUnit =
    (compilerOption.file, compilerOption.query) match
      case (Some(f), None) =>
        CompilationUnit.fromFile(s"${compilerOption.workFolder}/${f}".stripPrefix("./"))
      case (None, Some(q)) =>
        CompilationUnit.fromString(q)
      case _ =>
        throw StatusCode.INVALID_ARGUMENT.newException("Specify either --file or a query argument")

  private def compile(): CompileResult = compiler.compileSingleUnit(inputUnit)

  def generateSQL: String =
    val compileResult = compile()

    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(inputUnit)
      // Disable debug path as we can't run tests in plain SQL
      .withDebugRun(false).newContext(Symbol.NoSymbol)

    GenSQL.generateSQL(inputUnit, ctx)
  end generateSQL

  def run(): Unit =
    Control.withResource(QueryExecutor(dbConnector, compiler.compilerOptions.workEnv)) { executor =>
      val compileResult = compile()
      given Context     = compileResult.context

      val queryResult = executor.executeSelectedStatement(
        inputUnit,
        QuerySelection.All,
        nodeLocation = inputUnit.resolvedPlan.sourceLocation.nodeLocation,
        compileResult.context
      )
      println(queryResult.toPrettyBox())
    }

end WvletCompiler
