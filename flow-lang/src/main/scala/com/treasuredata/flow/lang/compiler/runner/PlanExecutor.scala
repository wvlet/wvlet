package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context}
import wvlet.log.LogSupport

object PlanExecutor:
  def inMemoryExecutor: PlanExecutor = PlanExecutor()

class PlanExecutor extends LogSupport:

  def execute(u: CompilationUnit, context: Context): Unit =
    debug(s"[${u.sourceFile.fileName}]\n\n${u.resolvedPlan.pp}")
    val result       = InMemoryExecutor.default.execute(u.resolvedPlan, context)
    val resultString = QueryResultPrinter.print(result)
    debug(resultString)
