/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.ui.playground

import wvlet.lang.api.Span
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.engine.EngineResult
import wvlet.lang.model.plan.*
import wvlet.lang.runner.TestEvaluator
import wvlet.lang.runner.TestFailure
import wvlet.lang.runner.TestSuccess
import wvlet.lang.runner.engine.EngineTableRows
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/** The outcome of one `test` assertion */
enum TestStatus:
  case Passed,
    Failed,
    Warning

/**
  * What a `test` or `debug` statement produced in a run, in source order. `line` is the 1-origin
  * source line of the statement, and `source` is its text: the line of a test, or the whole block
  * of a debug statement.
  */
enum Check(val line: Int, val source: String):
  case Test(
      override val line: Int,
      override val source: String,
      status: TestStatus,
      message: String
  ) extends Check(line, source)

  /** The rows a `debug { ... }` block produced, or why its query failed */
  case Debug(
      override val line: Int,
      override val source: String,
      result: Either[String, ResultTable]
  ) extends Check(line, source)

  def isFailure: Boolean =
    this match
      case t: Test =>
        t.status == TestStatus.Failed
      case d: Debug =>
        d.result.isLeft

/**
  * Runs every `test` and `debug` statement of a file on the engine, like `wvlet test`: a query runs
  * only when a test or a debug block reads its result, and a failure in one check never stops the
  * others.
  *
  * @param execute
  *   runs a query relation on the engine
  * @param toTable
  *   renders an engine result as display rows
  */
class PlaygroundChecks(
    unit: CompilationUnit,
    workEnv: WorkEnv,
    execute: Relation => EngineResult,
    toTable: EngineResult => ResultTable,
    rowLimit: Int
) extends LogSupport:

  private enum Step:
    case Query(r: Relation)
    case Test(t: TestRelation)
    case DebugBlock(d: Debug, steps: Seq[Step])

  // A query result, run on first use: tests of the same query share one run
  private class Pending(r: Relation):
    lazy val result: Either[String, EngineResult] =
      try
        Right(execute(r))
      catch
        case NonFatal(e) =>
          Left(PlaygroundChecks.errorMessage(e))

  /** Run the checks of the file. The context must be a debug-run context (`isDebugRun`). */
  def run()(using ctx: Context): Seq[Check] =
    val steps =
      try
        flatten(ExecutionPlanner.plan(unit, ctx))
      catch
        case NonFatal(e) =>
          warn(e)
          Nil
    if !steps.exists(hasCheck) then
      Nil
    else
      val out = Seq.newBuilder[Check]
      runSteps(steps, out)
      // Plans list an outer debug block before the inner one it wraps: show checks in source order
      out.result().sortBy(_.line)

  private def hasCheck(s: Step): Boolean =
    s match
      case _: Step.Query =>
        false
      case _ =>
        true

  private def flatten(p: ExecutionPlan): Seq[Step] =
    p match
      case ExecuteTasks(tasks) =>
        tasks.flatMap(flatten)
      case ExecuteQuery(r: Relation) =>
        Seq(Step.Query(r))
      case ExecuteTest(t) =>
        Seq(Step.Test(t))
      case ExecuteDebug(d, debugPlan) =>
        Seq(Step.DebugBlock(d, flatten(debugPlan)))
      // Saves, commands, and vals have side effects or are already defined by the run
      case _ =>
        Nil

  // Run steps in order; returns the last query result, for a debug block to show
  private def runSteps(steps: Seq[Step], out: mutable.Builder[Check, Seq[Check]])(using
      Context
  ): Option[Pending] =
    var last: Option[Pending] = None
    steps.foreach {
      case Step.Query(r) =>
        last = Some(Pending(r))
      case Step.Test(t) =>
        out += test(t, last)
      case Step.DebugBlock(d, inner) =>
        val nested = Seq.newBuilder[Check]
        val result = runSteps(inner, nested)
        val table  =
          result match
            case Some(p) =>
              p.result.map(toTable)
            case None =>
              Left("The debug block has no query to show")
        out += Check.Debug(lineOf(d.span), blockAt(d.span), table)
        out ++= nested.result()
    }
    last

  private def test(t: TestRelation, last: Option[Pending])(using Context): Check =
    val span                                        = t.testExpr.span
    def result(status: TestStatus, message: String) = Check.Test(
      lineOf(span),
      sourceAt(span),
      status,
      message
    )
    last.map(_.result) match
      case None =>
        result(TestStatus.Warning, "No query result to test")
      case Some(Left(err)) =>
        result(TestStatus.Failed, s"The tested query failed: ${err}")
      case Some(Right(rows)) =>
        try
          TestEvaluator.evaluate(t, EngineTableRows.of(rows, rowLimit), workEnv) match
            case s: TestSuccess =>
              result(TestStatus.Passed, s.msg)
            case f: TestFailure =>
              result(TestStatus.Failed, f.msg)
            case other =>
              result(TestStatus.Warning, other.toString)
        catch
          case NonFatal(e) =>
            result(TestStatus.Failed, PlaygroundChecks.errorMessage(e))

  private def lineOf(span: Span): Int =
    if span.nonEmpty && span.start >= 0 then
      unit.sourceFile.offsetToLine(span.start) + 1
    else
      0

  // The source lines of a block, from the line it starts at to its end, without common indentation
  private def blockAt(span: Span): String =
    if span.nonEmpty && span.start >= 0 then
      val text   = unit.sourceFile.getContentAsString
      val from   = text.lastIndexOf('\n', span.start - 1) + 1
      val lines  = text.slice(from, math.min(span.end, text.length)).linesIterator.toSeq
      val indent = lines
        .filter(_.trim.nonEmpty)
        .map(_.takeWhile(_ == ' ').length)
        .minOption
        .getOrElse(0)
      lines.map(_.drop(indent)).mkString("\n").trim
    else
      ""

  // The trimmed source line a statement starts at
  private def sourceAt(span: Span): String =
    if span.nonEmpty && span.start >= 0 then
      val text = unit.sourceFile.getContentAsString
      val from = text.lastIndexOf('\n', span.start - 1) + 1
      val to   =
        text.indexOf('\n', span.start) match
          case -1 =>
            text.length
          case n =>
            n
      text.slice(from, to).trim
    else
      ""

end PlaygroundChecks

object PlaygroundChecks:
  /** A one-line error message: compiler errors carry the source snippet on later lines */
  def errorMessage(e: Throwable): String =
    e match
      case _: wvlet.lang.engine.plan.ColumnNotFound =>
        s"[COLUMN_NOT_FOUND] ${e.getMessage}"
      case _: wvlet.lang.api.WvletLangException =>
        e.getMessage
      case _ =>
        s"${e.getClass.getSimpleName}: ${e.getMessage}"
