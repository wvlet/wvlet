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

import wvlet.lang.api.LinePosition
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.ValSymbolInfo
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.WvletGenerator
import wvlet.lang.compiler.lsp.CompletionItem
import wvlet.lang.compiler.lsp.CompletionProvider
import wvlet.lang.compiler.lsp.HoverProvider
import wvlet.lang.compiler.lsp.HoverResult
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.query.QuerySelector
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.StdLibExamples
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.catalog.EngineCatalog
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.TpchGen
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.ValueOps
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogLevel
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/** The language typed in the editor */
enum InputMode(val label: String, val editorLanguage: String):
  case Wvlet extends InputMode("Wvlet", "wvlet")
  case SQL   extends InputMode("SQL", "sql")

/** SQL dialects the playground can show the generated SQL in */
enum SqlDialect(val label: String, val dbType: DBType):
  case DuckDB    extends SqlDialect("DuckDB", DBType.DuckDB)
  case Trino     extends SqlDialect("Trino", DBType.Trino)
  case Snowflake extends SqlDialect("Snowflake", DBType.Snowflake)
  case Hive      extends SqlDialect("Hive", DBType.Hive)
  case BigQuery  extends SqlDialect("BigQuery", DBType.BigQuery)

/** A column of a result: its name and engine type name */
case class ResultColumn(name: String, typeName: String):
  def isNumeric: Boolean =
    val t = typeName.toLowerCase
    t == "int" || t == "long" || t == "double" || t.startsWith("decimal")

/** Query result rows rendered as display strings (None for NULL) */
case class ResultTable(
    columns: Seq[ResultColumn],
    rows: IndexedSeq[IndexedSeq[Option[String]]],
    totalRows: Int
):
  def isTruncated: Boolean = totalRows > rows.size

/** What the playground shows for one run: the translations, the plans, and the rows or error */
case class RunOutput(
    sql: String = "",
    wvlet: String = "",
    logicalPlan: String = "",
    // The engine's runtime plan as bound from the query, and after its logical passes
    boundPlan: String = "",
    runtimePlan: String = "",
    // How many times each optimizer rule changed the runtime plan, most first
    firedRules: Seq[(String, Int)] = Nil,
    physicalPlan: String = "",
    result: Option[ResultTable] = None,
    error: Option[String] = None,
    elapsedMillis: Double = 0,
    // Planning and running on the engine only, without compilation
    executionMillis: Double = 0,
    statement: String = "",
    // The 1-origin line of the statement that ran
    statementLine: Option[Int] = None,
    // How the file's tables, seeds, models, and queries read each other
    lineage: ModelLineage = ModelLineage(),
    // The results of the file's `test` and `debug` statements, in execution order
    checks: Seq[Check] = Nil
):
  def hasError: Boolean = error.nonEmpty

  /** The `test` results of the run */
  def tests: Seq[Check.Test] = checks.collect { case t: Check.Test =>
    t
  }

  def failedTests: Int = tests.count(_.status == TestStatus.Failed)

  /** Failed tests and debug blocks whose query failed */
  def failedChecks: Int = checks.count(_.isFailure)

  /** The rows of the run's `debug` blocks */
  def debugBlocks: Seq[Check.Debug] = checks.collect { case d: Check.Debug =>
    d
  }

end RunOutput

/**
  * Compiles and runs queries of the playground on the in-process columnar engine, over TPC-H tables
  * generated in memory ([[TpchGen]]). It needs no server and no DuckDB, so it runs in the browser.
  *
  * @param scaleFactor
  *   the TPC-H scale factor of the generated tables
  */
class PlaygroundEngine(val scaleFactor: Double = PlaygroundEngine.defaultScaleFactor)
    extends LogSupport:
  val tables: Seq[MemTable] = TpchGen.tables(scaleFactor)
  private val engine        = WvletEngine(optimizer = OptimizerOptions())
  tables.foreach(engine.catalog.register)

  // One compiler per dialect: the catalog's dialect decides the SQL a compiler generates. The
  // `wvlet` one compiles the queries the engine runs
  private val compilers = mutable.Map.empty[DBType, Compiler]

  private def compilerFor(dbType: DBType): Compiler = compilers.getOrElseUpdate(
    dbType,
    newCompiler(dbType)
  )

  private def newCompiler(dbType: DBType): Compiler =
    val c = Compiler(
      CompilerOptions(
        sourceFolders = Nil,
        workEnv = WorkEnv(".", logLevel = LogLevel.WARN),
        dbType = dbType
      )
    )
    c.setDefaultCatalog(EngineCatalog.toCompileCatalog(tables, dbType))
    c.setDefaultSchema(EngineCatalog.SchemaName)
    c

  // Hovers and completions compile partial text on every request; a compiler of their own keeps
  // those units out of the one that runs queries
  private lazy val editorCompiler = newCompiler(DBType.Wvlet)

  /**
    * The editor tooltip at a 0-origin `offset` of Wvlet `text`: a column's type, a step's schema,
    * or a stdlib function's signatures and docs
    */
  def hover(text: String, offset: Int): Option[HoverResult] =
    try
      HoverProvider.hover(text, offset, editorCompiler)
    catch
      case NonFatal(e) =>
        None

  private lazy val stdlibExamples = StdLibExamples(engine, newCompiler(DBType.Wvlet))
  private val exampleResults      = mutable.Map.empty[String, Either[String, String]]

  /** The result of a stdlib `@example` on the engine (cached), or Left with the error */
  def runExample(example: String): Either[String, String] = exampleResults.getOrElseUpdate(
    example,
    stdlibExamples.run(example)
  )

  /** Completion candidates at a 0-origin `offset`: keywords, definitions, columns, or members */
  def complete(text: String, offset: Int): List[CompletionItem] =
    try
      CompletionProvider.complete(text, offset, editorCompiler)
    catch
      case NonFatal(e) =>
        Nil

  // The engine with some optimizations turned off, over the same tables
  private def engineFor(disabled: Set[Optimization]): WvletEngine =
    if disabled.isEmpty then
      engine
    else
      engine.withOptimizer(OptimizerOptions(disabled = disabled))

  /** Columns of the generated tables, for the schema browser */
  def tableSchemas: Seq[(String, Seq[ResultColumn])] = tables.map { t =>
    t.name -> t.schema.fields.map(f => ResultColumn(f.name, f.ptype.name))
  }

  /** Row counts of the generated tables */
  def tableRows(name: String): Long = tables
    .find(_.name == name)
    .flatMap(_.estimatedRows)
    .getOrElse(0L)

  /**
    * Compile `text` and run the statement at `position` (the last one when empty) on the engine.
    *
    * @param selection
    *   which part of the statement at `position` to run: the statement, the innermost subquery, or
    *   its schema (describe)
    */
  def run(
      text: String,
      mode: InputMode = InputMode.Wvlet,
      dialect: SqlDialect = SqlDialect.DuckDB,
      position: LinePosition = LinePosition.NoPosition,
      selection: QuerySelection = QuerySelection.Single,
      rowLimit: Int = PlaygroundEngine.defaultRowLimit,
      disabled: Set[Optimization] = Set.empty
  ): RunOutput =
    val start   = System.nanoTime()
    def elapsed = (System.nanoTime() - start) / 1e6
    var out     = RunOutput()
    val unit    = newUnit(text, mode)
    try
      val r = compilerFor(DBType.Wvlet).compileSingleUnit(unit)
      if r.hasFailures then
        throw r.failureException
      given ctx: Context = r
        .context
        .withCompilationUnit(unit)
        .withDebugRun(false)
        .newContext(Symbol.NoSymbol)
      val selected = QuerySelector.selectQuery(unit, position, selection)
      out = out.copy(
        statement = statementText(unit, selected),
        statementLine = statementLine(unit, selected),
        lineage = lineageOf(unit),
        logicalPlan = LogicalPlanPrinter.print(selected),
        wvlet =
          if mode == InputMode.SQL then
            toWvlet(text)
          else
            ""
      )
      // A dialect that can't express the query (e.g., asof join on Trino) only affects the SQL view
      val sql =
        try
          translate(text, mode, dialect, position, selection)
        catch
          case e: WvletLangException =>
            s"-- Cannot generate ${dialect.label} SQL: ${e
                .getMessage
                .linesIterator
                .nextOption()
                .getOrElse("")}"
      out = out.copy(sql = sql)
      val relations = runnable(unit, selected)
      val target    = engineFor(disabled)
      relations.lastOption match
        case None =>
          out.copy(
            result = Some(ResultTable(Nil, IndexedSeq.empty, 0)),
            elapsedMillis = elapsed,
            checks = checksOf(unit, target, rowLimit, Map.empty)
          )
        case Some(rel) =>
          val stages = target.planStages(rel)
          try
            out = out.copy(
              boundPlan = stages.bound.map(_.explain).getOrElse(""),
              runtimePlan = stages.optimized.map(_.explain).getOrElse(""),
              firedRules = stages.firedRules.toSeq.sortBy((name, n) => (-n, name)),
              physicalPlan = stages.physical.explain()
            )
          finally stages.close()
          val execStart = System.nanoTime()
          val res       =
            try
              Right(target.execute(rel))
            catch
              case NonFatal(e) =>
                Left(e)
          val execMillis = (System.nanoTime() - execStart) / 1e6
          val runMillis  = elapsed
          // Checks run after the timed run, and reuse its result for tests of the same query
          out = out.copy(checks =
            checksOf(unit, target, rowLimit, res.toOption.map(rel -> _).toMap)
          )
          res match
            case Left(e) =>
              throw e
            case Right(r) =>
              out.copy(
                result = Some(toTable(r, rowLimit)),
                elapsedMillis = runMillis,
                executionMillis = execMillis
              )
      end match
    catch
      // A compile error, or a column the engine can't bind, is a mistake in the query
      case e @ (_: WvletLangException | _: wvlet.lang.engine.plan.ColumnNotFound) =>
        out.copy(error = Some(PlaygroundChecks.errorMessage(e)), elapsedMillis = elapsed)
      case NonFatal(e) =>
        warn(e)
        out.copy(error = Some(PlaygroundChecks.errorMessage(e)), elapsedMillis = elapsed)
    finally
      // Each run compiles a new unit on a long-lived compiler: release it, or the vals it defined
      // stay in the compiler and hide the next run's (`Table not found: holding` on a re-run)
      compilerFor(DBType.Wvlet).releaseUnit(unit)
    end try

  end run

  /** Convert a SQL query to Wvlet */
  def toWvlet(sql: String): String =
    val unit = CompilationUnit.fromSqlString(sql)
    // Parse only: the Wvlet text follows the SQL as written
    val r = Compiler.parseOnly(compilerFor(DBType.Wvlet).compilerOptions).compileSingleUnit(unit)
    if r.hasFailures then
      throw r.failureException
    given ctx: Context = r.context.withCompilationUnit(unit)
    WvletGenerator(CodeFormatterConfig(sqlDBType = DBType.DuckDB)).print(unit.resolvedPlan)

  // The SQL of the selected statement in `dialect`
  private def translate(
      text: String,
      mode: InputMode,
      dialect: SqlDialect,
      position: LinePosition,
      selection: QuerySelection
  ): String =
    val unit     = newUnit(text, mode)
    val compiler = compilerFor(dialect.dbType)
    try
      val r = compiler.compileSingleUnit(unit)
      if r.hasFailures then
        throw r.failureException
      given ctx: Context = r
        .context
        .withCompilationUnit(unit)
        .withDebugRun(false)
        .newContext(Symbol.NoSymbol)
      val selected = QuerySelector.selectQuery(unit, position, selection)
      // Drop the `-- wvlet version=..., src=...` header line of each statement
      GenSQL
        .generateSQL(unit, targetPlan = Some(selected))
        .linesIterator
        .filterNot(_.startsWith("-- wvlet version="))
        .mkString("\n")
    finally
      compiler.releaseUnit(unit)

  private def newUnit(text: String, mode: InputMode): CompilationUnit =
    mode match
      case InputMode.Wvlet =>
        CompilationUnit.fromWvletString(text)
      case InputMode.SQL =>
        CompilationUnit.fromSqlString(text)

  private def statementText(unit: CompilationUnit, plan: LogicalPlan): String =
    plan match
      case _: PackageDef =>
        ""
      case p =>
        val text = unit.sourceFile.getContentAsString
        val s    = p.span
        if s.nonEmpty && s.start >= 0 && s.end <= text.length then
          text.slice(s.start, s.end).linesIterator.nextOption().getOrElse("").trim
        else
          ""

  private def statementLine(unit: CompilationUnit, plan: LogicalPlan): Option[Int] =
    plan match
      case _: PackageDef =>
        None
      case p =>
        ModelLineage.lineOf(unit, p)

  // The lineage is a view of the file: a failure to derive it must not fail the run
  private def lineageOf(unit: CompilationUnit)(using Context): ModelLineage =
    try
      ModelLineage.of(unit, tables.map(_.name).toSet)
    catch
      case NonFatal(e) =>
        warn(e)
        ModelLineage()

  // The queries to run for the selected plan, after defining the vals it reads
  private def runnable(unit: CompilationUnit, selected: LogicalPlan)(using
      ctx: Context
  ): Seq[Relation] =
    val out                          = Seq.newBuilder[Relation]
    def loop(p: ExecutionPlan): Unit =
      p match
        case ExecuteTasks(tasks) =>
          tasks.foreach(loop)
        case ExecuteQuery(r: Relation) =>
          out += r
        case ExecuteSave(_, q) =>
          loop(q)
        case ExecuteValDef(v) =>
          val expr = ExpressionEvaluator.eval(v.expr)
          v.symbol.symbolInfo = ValSymbolInfo(
            ctx.owner,
            v.symbol,
            v.name,
            expr.dataType,
            expr,
            ctx.compilationUnit
          )
          ctx.enter(v.symbol)
        case _ =>
    // Vals defined anywhere in the file are visible to the selected statement
    if !selected.isInstanceOf[PackageDef] then
      ExecutionPlanner
        .plan(unit, ctx)
        .match
          case ExecuteTasks(ts) =>
            ts.foreach {
              case v: ExecuteValDef =>
                loop(v)
              case _ =>
            }
          case v: ExecuteValDef =>
            loop(v)
          case _ =>
    loop(ExecutionPlanner.plan(unit, selected))
    out.result()

  end runnable

  // Every `test` and `debug` of the file, run like `wvlet test` does. A failure here is shown as
  // a check, and never fails the run of the selected statement. `known` holds results already run
  private def checksOf(
      unit: CompilationUnit,
      target: WvletEngine,
      rowLimit: Int,
      known: Map[Relation, EngineResult]
  )(using ctx: Context): Seq[Check] =
    val checks = PlaygroundChecks(
      unit,
      compilerFor(DBType.Wvlet).compilerOptions.workEnv,
      // Plans are case classes: an equal relation is the same query
      r => known.getOrElse(r, target.execute(r)),
      res => toTable(res, rowLimit),
      rowLimit
    )
    checks.run()(using ctx.withDebugRun(true))

  private def toTable(res: EngineResult, rowLimit: Int): ResultTable =
    val types = res.schema.types
    val cols  = res.schema.fields.map(f => ResultColumn(f.name, f.ptype.name))
    val rows  =
      res
        .rows
        .take(rowLimit)
        .map { r =>
          r.indices
            .map { i =>
              Option(r(i)).map(v => PlaygroundEngine.display(ValueOps.toExternal(v, types(i))))
            }
        }
        .toIndexedSeq
    ResultTable(cols, rows, res.numRows)

end PlaygroundEngine

object PlaygroundEngine:
  val defaultScaleFactor: Double = 0.01
  val defaultRowLimit: Int       = 1000

  /** Scale factors offered in the UI */
  val scaleFactors: Seq[Double] = Seq(0.001, 0.01, 0.05, 0.1)

  /** Render an engine value as text, as the CLI prints it */
  def display(v: Any): String =
    v match
      case null =>
        "null"
      case s: String =>
        s
      case xs: Seq[?] =>
        xs.map(x => quoteIfString(x)).mkString("[", ", ", "]")
      case xs: Array[?] =>
        display(xs.toSeq)
      case m: Map[?, ?] =>
        m.map((k, x) => s"${quoteIfString(k)}: ${quoteIfString(x)}").mkString("{", ", ", "}")
      case d: Double =>
        formatDouble(d)
      case other =>
        other.toString

  /** Decimal places shown for doubles; decimals keep the scale of their type */
  val doubleDigits: Int = 6

  /**
    * A double rounded to [[doubleDigits]] decimal places, without trailing zeros (`35893.865307`
    * instead of `35893.86530718333`). Very large or small magnitudes use scientific notation
    */
  def formatDouble(d: Double): String =
    if d.isNaN || d.isInfinite then
      d.toString
    else if d != 0 && (math.abs(d) >= 1e15 || math.abs(d) < 1e-4) then
      String.format("%.6e", java.lang.Double.valueOf(d))
    else
      val s =
        BigDecimal(d)
          .setScale(doubleDigits, BigDecimal.RoundingMode.HALF_UP)
          .bigDecimal
          .stripTrailingZeros
          .toPlainString
      if s.contains('.') then
        s
      else
        s"${s}.0"

  private def quoteIfString(x: Any): String =
    x match
      case s: String =>
        s"'${s}'"
      case other =>
        display(other)

end PlaygroundEngine
