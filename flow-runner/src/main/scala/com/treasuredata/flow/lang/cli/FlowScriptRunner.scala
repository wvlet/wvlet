package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{
  CompilationUnit,
  CompileResult,
  Compiler,
  CompilerOptions
}
import com.treasuredata.flow.lang.runner.*
import org.jline.terminal.Terminal
import wvlet.airframe.control.{Control, Shell}
import wvlet.log.LogSupport

import java.awt.Toolkit
import java.io.{BufferedWriter, FilterOutputStream, OutputStreamWriter}

case class FlowScriptRunnerConfig(
    workingFolder: String = ".",
    interactive: Boolean,
    resultLimit: Int = 40,
    catalog: Option[String],
    schema: Option[String]
)

case class LastOutput(line: String, output: String) {}

class FlowScriptRunner(config: FlowScriptRunnerConfig, queryExecutor: QueryExecutor)
    extends AutoCloseable
    with LogSupport:
  private var units: List[CompilationUnit] = Nil

  private var resultRowLimits: Int = config.resultLimit

  def setResultRowLimit(limit: Int): Unit = resultRowLimits = limit

  override def close(): Unit = queryExecutor.close()

  private val compiler = Compiler(
    CompilerOptions(
      sourceFolders = List(config.workingFolder),
      workingFolder = config.workingFolder,
      catalog = config.catalog,
      schema = config.schema
    )
  )

  def runStatement(line: String, terminal: Terminal): LastOutput =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = compiler.compileSingle(contextUnit = newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = queryExecutor.execute(newUnit, ctx, limit = resultRowLimits)
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
          val proc = ProcessUtil.launchInteractiveProcess("less", "-FXRSn")
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

      LastOutput(line, str)
    catch
      case e: FlowLangException if e.statusCode.isUserError =>
        error(s"${e.getMessage}")
        LastOutput(line, e.getMessage)
    end try

  end runStatement

end FlowScriptRunner
