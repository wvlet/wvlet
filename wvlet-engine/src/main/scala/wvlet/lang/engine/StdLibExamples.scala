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
package wvlet.lang.engine

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.ValueOps
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogLevel

import java.math.BigDecimal as JBigDecimal
import scala.util.control.NonFatal

/**
  * Runs the `-- @example` snippets of stdlib functions on the engine, so the function reference
  * (the docs page and the playground) shows results the engine computed, never hand-written ones.
  *
  * An example is an expression (`'Wvlet'.upper`, run as `select <expr>`) or a whole query starting
  * with `from` or `select` (`from [[1], [2]] as t(x) agg x.sum`). Its result is the values of the
  * first column, joined with `, `; multi-column rows show as `(a, b)`.
  */
class StdLibExamples(
    engine: WvletEngine = WvletEngine(),
    compiler: Compiler = StdLibExamples.newCompiler()
):

  /** The result of an example as text, or Left with the error message */
  def run(example: String): Either[String, String] =
    val unit = CompilationUnit.fromWvletString(StdLibExamples.queryOf(example))
    try
      val r = compiler.compileSingleUnit(unit)
      if r.hasFailures then
        Left(r.failureException.getMessage)
      else
        given ctx: Context = r
          .context
          .withCompilationUnit(unit)
          .withDebugRun(false)
          .newContext(Symbol.NoSymbol)
        StdLibExamples.relationOf(unit.executionPlan) match
          case None =>
            Left("The example has no query")
          case Some(rel) =>
            val res = engine.execute(rel)
            Right(StdLibExamples.format(res))
    catch
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    finally
      compiler.releaseUnit(unit)

end StdLibExamples

object StdLibExamples:
  // Types whose external values are text but print without quotes
  private val textTypes: PType => Boolean =
    case PType.Date | PType.Timestamp | PType.TimestampTz | _: PType.Decimal =>
      true
    case _ =>
      false

  /** Rows shown at most for an example that returns many */
  val maxRows: Int = 10

  /** A compiler for examples: inline values only, with the wvlet engine's stdlib variants */
  def newCompiler(): Compiler = Compiler(
    CompilerOptions(
      sourceFolders = Nil,
      workEnv = WorkEnv(".", logLevel = LogLevel.WARN),
      dbType = DBType.Wvlet
    )
  )

  /** The query an example runs: the example itself, or `select <expr>` for an expression */
  def queryOf(example: String): String =
    val t = example.trim
    if t.startsWith("from ") || t.startsWith("select ") then
      t
    else
      s"select ${t}"

  private def relationOf(p: ExecutionPlan): Option[Relation] =
    p match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(relationOf).nextOption()
      case ExecuteSave(_, q) =>
        relationOf(q)
      case _ =>
        None

  /** The rows of a result as one line of text */
  def format(res: EngineResult): String =
    val types = res.schema.types
    val rows  =
      res
        .rows
        .take(maxRows + 1)
        .map { r =>
          val cells = r.indices.map(i => value(ValueOps.toExternal(r(i), types(i)), types(i)))
          if cells.size == 1 then
            cells.head
          else
            cells.mkString("(", ", ", ")")
        }
        .toList
    val shown = rows.take(maxRows).mkString(", ")
    if rows.size > maxRows then
      s"${shown}, …"
    else
      shown

  // A value as a literal: strings quoted, so 'a' and a column name don't look alike
  private def value(v: Any, t: PType): String =
    v match
      case null =>
        "null"
      case s: String if StdLibExamples.textTypes(t) =>
        // Dates, timestamps, and decimals come as text
        s
      case s: String =>
        quote(s)
      case _ =>
        nested(v)

  private def nested(v: Any): String =
    v match
      case null =>
        "null"
      case s: String =>
        quote(s)
      case xs: Seq[?] =>
        xs.map(nested).mkString("[", ", ", "]")
      case m: scala.collection.Map[?, ?] =>
        m.map((k, x) => s"${nested(k)}: ${nested(x)}").mkString("{", ", ", "}")
      case d: Double =>
        formatDouble(d)
      case f: Float =>
        formatDouble(f.toDouble)
      case b: JBigDecimal =>
        b.toPlainString
      case other =>
        other.toString

  private def quote(s: String): String = s"'${s.replace("'", "''")}'"

  /** A double with at most 6 decimal places and no trailing zeros */
  def formatDouble(d: Double): String =
    if d.isNaN || d.isInfinite then
      d.toString
    else if d == math.rint(d) && math.abs(d) < 1e15 then
      s"${d.toLong}.0"
    else
      BigDecimal(d)
        .setScale(6, BigDecimal.RoundingMode.HALF_UP)
        .bigDecimal
        .stripTrailingZeros()
        .toPlainString

end StdLibExamples
