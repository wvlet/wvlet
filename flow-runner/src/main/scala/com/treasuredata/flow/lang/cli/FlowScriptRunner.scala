package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{CompilationUnit, CompileResult, Compiler}
import com.treasuredata.flow.lang.runner.{
  DuckDBExecutor,
  PlanResult,
  QueryResult,
  QueryResultList,
  QueryResultPrinter
}
import org.jline.terminal.Terminal
import wvlet.airframe.control.{Control, Shell}
import wvlet.log.LogSupport

import java.io.{BufferedWriter, FilterOutputStream, OutputStreamWriter}

case class FlowScriptRunnerConfig(
    workingFolder: String = ".",
    interactive: Boolean,
    resultLimit: Int = 40
)

class FlowScriptRunner(config: FlowScriptRunnerConfig) extends AutoCloseable with LogSupport:
  private val duckDBExecutor               = new DuckDBExecutor()
  private var units: List[CompilationUnit] = Nil

  override def close(): Unit = duckDBExecutor.close()

  private val compiler = Compiler(
    sourceFolders = List(config.workingFolder),
    contextFolder = config.workingFolder
  )

  def runStatement(line: String, terminal: Terminal): Unit =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = compiler.compileSingle(contextUnit = newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = duckDBExecutor.execute(newUnit, ctx, limit = config.resultLimit)
      trace(s"ctx: ${ctx.hashCode()} ${ctx.compilationUnit.knownSymbols}")

      def resultString(q: QueryResult): String =
        q match
          case PlanResult(plan, result) =>
            QueryResultPrinter.print(result)
          case QueryResultList(list) =>
            list.map(x => resultString(x)).mkString("\n\n")
          case other =>
            QueryResultPrinter.print(other)

      val str = resultString(queryResult)
      if str.nonEmpty then

        val maxWidth = str.split("\n").map(_.size).max
        if !config.interactive || maxWidth <= terminal.getWidth then
          println(str)
        else
          val proc = ProcessBuilder("less", "-FXRSn")
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start()
          val out =
            new BufferedWriter(
              new OutputStreamWriter(
                // Need to use a FilterOutputStream to accept keyboard events for less command
                new FilterOutputStream(proc.getOutputStream())
              )
            )
          out.write(str)
          out.flush()
          out.close()
          // Blocking
          proc.waitFor()

    catch
      case e: FlowLangException if e.statusCode.isUserError =>
        error(s"${e.getMessage}")
    end try

  end runStatement

end FlowScriptRunner
