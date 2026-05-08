package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.WvletGenerator
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.log.LogSupport

/**
  * Compile-only frontend to the wvlet compiler. Mirrors the JVM `wvlet-cli/WvletCompiler`'s
  * `generateSQL` / `generateWvlet` paths but skips `Profile` / `DBConnectorProvider` plumbing —
  * those live in `wvlet-runner` (JVM-only) and are layered on top by the JVM CLI when the user
  * passes `--profile`. This shared core works on JVM, Node.js, and Native.
  */
class WvletCliCompiler(opt: WvletCliCompileOption) extends LogSupport:

  private def dbType: DBType = opt.targetDBType.map(DBType.fromString).getOrElse(DBType.DuckDB)

  private def createCompiler(parseOnly: Boolean = false): Compiler =
    val options = CompilerOptions(
      sourceFolders = List(opt.workFolder),
      workEnv = WorkEnv(opt.workFolder),
      dbType = dbType
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

  def generateWvlet: String =
    val inputUnit = getInputUnit(forSQL = true)
    val ctx       = compileInternal(inputUnit, parseOnly = inputUnit.sourceFile.isSQL)
    val config    = CodeFormatterConfig(sqlDBType = ctx.dbType)
    val generator = WvletGenerator(config)(using ctx)
    generator.print(inputUnit.resolvedPlan)

end WvletCliCompiler
