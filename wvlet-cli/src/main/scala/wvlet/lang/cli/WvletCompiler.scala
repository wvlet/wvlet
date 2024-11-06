package wvlet.lang.cli

import wvlet.airframe.launcher.{argument, option}
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, Symbol, WorkEnv}
import wvlet.lang.model.plan.ExecutionPlan
import wvlet.log.LogSupport

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

class WvletCompiler(opts: WvletGlobalOption, compilerOption: WvletCompilerOption)
    extends LogSupport:

  def toSQL: String =
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
    val ctx = compileResult.context.withCompilationUnit(inputUnit).newContext(Symbol.NoSymbol)
    val executionPlan = ExecutionPlanner.plan(inputUnit, ctx)
    toSQL(executionPlan)
  end toSQL

  private def toSQL(executionPlan: ExecutionPlan): String =
    info(executionPlan.pp)
    ""

end WvletCompiler
