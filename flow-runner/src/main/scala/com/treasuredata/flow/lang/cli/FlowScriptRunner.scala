package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{CompilationUnit, CompileResult, Compiler}
import com.treasuredata.flow.lang.runner.{
  DuckDBExecutor,
  QueryResult,
  PlanResult,
  QueryResultList,
  QueryResultPrinter
}
import wvlet.log.LogSupport

case class FlowScriptRunnerConfig(workingFolder: String = ".")

class FlowScriptRunner(config: FlowScriptRunnerConfig) extends AutoCloseable with LogSupport:
  private lazy val duckDBExecutor          = new DuckDBExecutor()
  private var units: List[CompilationUnit] = Nil

  override def close(): Unit = duckDBExecutor.close()

  private val compiler = Compiler(
    sourceFolders = List(config.workingFolder),
    contextFolder = config.workingFolder
  )

  def runStatement(line: String): Unit =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = compiler.compileSingle(contextUnit = newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = duckDBExecutor.execute(newUnit, ctx)
      trace(s"ctx: ${ctx.hashCode()} ${ctx.compilationUnit.knownSymbols}")

      def resultString(q: QueryResult): String =
        q match
          case PlanResult(plan, result) =>
            QueryResultPrinter.print(result, limit = Some(10))
          case QueryResultList(list) =>
            list.map(x => resultString(x)).mkString("\n\n")
          case other =>
            QueryResultPrinter.print(other, limit = Some(10))

      val str = resultString(queryResult)
      if str.nonEmpty then
        println(str)
    catch
      case e: FlowLangException if e.statusCode.isUserError =>
        error(s"${e.getMessage}")
    end try

  end runStatement

end FlowScriptRunner
