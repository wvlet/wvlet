package com.treasuredata.flow.lang.cli

import com.treasuredata.flow.lang.FlowLangException
import com.treasuredata.flow.lang.compiler.{CompilationUnit, CompileResult, Compiler}
import com.treasuredata.flow.lang.runner.{DuckDBExecutor, QueryResultPrinter}
import wvlet.log.LogSupport

case class FlowScriptRunnerConfig()

class FlowScriptRunner(config: FlowScriptRunnerConfig) extends AutoCloseable with LogSupport:
  private val duckDBExecutor               = new DuckDBExecutor()
  private var units: List[CompilationUnit] = Nil

  override def close(): Unit = duckDBExecutor.close()

  def runStatement(line: String): Unit =
    val newUnit = CompilationUnit.fromString(line)
    units = newUnit :: units

    try
      val compileResult = Compiler.default.compileSingle(newUnit)
      val ctx           = compileResult.context.global.getContextOf(newUnit)
      val queryResult   = duckDBExecutor.execute(newUnit, ctx)
      val resultString  = QueryResultPrinter.print(queryResult, limit = Some(10))
      println(resultString)
    catch
      case e: FlowLangException if e.statusCode.isUserError =>
        error(s"${e.getMessage}")
