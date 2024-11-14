package wvlet.lang.native

import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, Symbol, WorkEnv}
import wvlet.log.LogSupport

object WvcMain extends LogSupport:

  def main(args: Array[String]): Unit =
    var inputQuery: Option[String] = None

    // TODO Use generic command line parser (airframe-launcher still doesn't support Scala Native)
    // Parse -c (query) option
    args.toList match
      case "-c" :: query :: _ =>
        inputQuery = Some(query.toString)
      case _ =>

    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv()))
    val inputUnit =
      inputQuery match
        case Some(q) =>
          CompilationUnit.fromString(q)
        case None =>
          warn("No query is given. Use -c option to specify a query")
          CompilationUnit.fromString("select 1")

    val compileResult = compiler.compileSingleUnit(inputUnit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(inputUnit)
      .withDebugRun(false)
      .newContext(Symbol.NoSymbol)

    val sql = GenSQL.generateSQL(inputUnit, ctx)
    println(sql)

end WvcMain
