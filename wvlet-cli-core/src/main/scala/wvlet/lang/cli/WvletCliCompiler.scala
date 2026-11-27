package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.WvletGenerator
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.log.LogSupport

/**
  * Compiler frontend of the npm and Native `wvlet`. It compiles with the dialect, catalog, and
  * schema of the profile's default engine, without connecting to it: the JVM CLI's `WvletCompiler`
  * also reads table schemas through the JVM connectors. This shared core works on JVM, Node.js, and
  * Native.
  *
  * @param profile
  *   the resolved profile. The single-argument constructor uses the one `opt` selects, with DuckDB
  *   SQL when neither `-t` nor `-p` is given
  */
class WvletCliCompiler(opt: WvletCliCompileOption, profile: Profile) extends LogSupport:

  def this(opt: WvletCliCompileOption) = this(opt, opt.resolveProfile(Profile.defaultDuckDBProfile))

  private def engine = profile.defaultEngine

  private def createCompiler(parseOnly: Boolean = false): Compiler =
    val options = CompilerOptions(
      sourceFolders = List(opt.workFolder),
      workEnv = WorkEnv(opt.workFolder),
      catalog = engine.catalog,
      schema = engine.schema,
      dbType = engine.dbType,
      failOnTypeErrors = opt.strict
    )
    if parseOnly then
      Compiler.parseOnly(options)
    else
      Compiler(options)

  private def getInputUnit(forSQL: Boolean): CompilationUnit =
    (opt.file, opt.query) match
      case (Some(f), None) =>
        CompilationUnit.fromFile(s"${opt.workFolder}/${f}".stripPrefix("./"))
      case (None, Some(q)) =>
        if forSQL then
          CompilationUnit.fromSqlString(q)
        else
          CompilationUnit.fromWvletString(q)
      case _ =>
        throw StatusCode.INVALID_ARGUMENT.newException("Specify either --file or a query argument")

  private def compileInternal(inputUnit: CompilationUnit, parseOnly: Boolean = false): Context =
    val compiler      = createCompiler(parseOnly = parseOnly)
    val compileResult = compiler.compileSingleUnit(inputUnit)
    compileResult.reportAllErrors
    compileResult
      .context
      .withCompilationUnit(inputUnit)
      // Disable debug path as we can't run tests in plain SQL
      .withDebugRun(false)
      .newContext(Symbol.NoSymbol)

  def generateSQL: String =
    val inputUnit = getInputUnit(forSQL = false)
    val ctx       = compileInternal(inputUnit)
    GenSQL.generateSQL(inputUnit)(using ctx)

  /**
    * Compile for execution: unlike [[generateSQL]], the debug path stays enabled so `test` and
    * `debug` statements are planned and evaluated by the runner.
    */
  def compileForRun: (CompilationUnit, Context) =
    val inputUnit     = getInputUnit(forSQL = false)
    val compiler      = createCompiler()
    val compileResult = compiler.compileSingleUnit(inputUnit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(inputUnit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    (inputUnit, ctx)

  def generateWvlet: String =
    val inputUnit = getInputUnit(forSQL = true)
    val ctx       = compileInternal(inputUnit, parseOnly = inputUnit.sourceFile.isSQL)
    val config    = CodeFormatterConfig(sqlDBType = ctx.dbType)
    val generator = WvletGenerator(config)(using ctx)
    generator.print(inputUnit.resolvedPlan)

end WvletCliCompiler
