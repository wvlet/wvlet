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

import wvlet.lang.engine.runtime.PlanAccelerator

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.*

import wvlet.uni.log.LogSupport

import scala.collection.immutable.ListMap

/** Rows produced by the engine */
case class EngineResult(schema: Schema, batches: Seq[Batch]):
  def numRows: Int = batches.map(_.size).sum

  /** Engine values per row */
  def rows: Iterator[IndexedSeq[Any]] = batches
    .iterator
    .flatMap(b => Iterator.range(0, b.size).map(i => b.columns.toIndexedSeq.map(_.get(i))))

  /**
    * Rows in the shape the runner's result printers expect: integers as Long, decimals and dates as
    * strings
    */
  def toRowMaps(limit: Int = Int.MaxValue): Seq[ListMap[String, Any]] =
    val names = schema.names
    toExternalRows(limit).map(r => ListMap.from(names.indices.map(i => names(i) -> r(i))))

  /** The same values as [[toRowMaps]], by column position */
  def toExternalRows(limit: Int = Int.MaxValue): Seq[IndexedSeq[Any]] =
    val types = schema.types
    rows.take(limit).map(r => r.indices.map(i => ValueOps.toExternal(r(i), types(i)))).toSeq

/**
  * The planning stages of one query ([[WvletEngine.planStages]]). `bound` and `optimized` are empty
  * when the query planned without a runtime plan of its own (e.g., a plan-time constant)
  */
case class EnginePlanStages(
    bound: Option[wvlet.lang.engine.runtime.RuntimePlan],
    optimized: Option[wvlet.lang.engine.runtime.RuntimePlan],
    firedRules: Map[String, Int],
    physical: Operator
):
  def close(): Unit = physical.close()

/**
  * The in-process columnar query engine: executes typed wvlet relations on vectors, reading Parquet
  * / Iceberg through uni. Tables are registered in [[catalog]]; file paths in queries are resolved
  * against `baseDir`. `optimizer` selects the planner optimizations to run (by default, all but
  * those named in the `wvlet.engine.optimizer.disabled` system property). With an `accelerator`,
  * the plan subtrees it supports run on it (e.g. a GPU), and the rest on the CPU.
  *
  * Each query reserves the bytes of its large structures from `memoryPool` (by default the
  * process's, shared by the engines of a server) and fails with RESOURCE_EXHAUSTED past
  * `queryMemoryLimit` bytes, or the pool's per-query limit (`memory_accounting`).
  */
class WvletEngine(
    val catalog: EngineCatalog = EngineCatalog(),
    baseDir: String = ".",
    val optimizer: OptimizerOptions = OptimizerOptions.fromSystem(),
    val accelerator: Option[PlanAccelerator] = None,
    val memoryPool: MemoryPool = MemoryPool.process,
    val queryMemoryLimit: Option[Long] = None
) extends LogSupport:

  private class DefaultResolver(using ctx: Context) extends SourceResolver:
    def tableSource(name: String, fullName: String): Option[TableSource] = catalog
      .informationSchema(fullName)

    def fileSource(path: String): Option[TableSource] =
      val resolved = resolvePath(path)
      val lower    = path.toLowerCase
      val name     = path.split("[/\\\\]").last.takeWhile(_ != '.')
      if lower.endsWith(".parquet") || lower.contains("*.parquet") ||
        lower.endsWith(".metadata.json") || SourceIO.isDirectory(resolved)
      then
        Some(ParquetSource(name, resolved, optimizer.isEnabled(Optimization.ParquetFooterCache)))
      else if lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson") ||
        lower.endsWith(".json.gz") || lower.endsWith(".jsonl.gz")
      then
        Some(JsonSource(name, resolved))
      else if lower.endsWith(".csv") || lower.endsWith(".tsv") || lower.endsWith(".csv.gz") then
        Some(CsvSource(name, resolved))
      else
        None

    def parseSql(r: RawSQL): Relation =
      val sql  = SqlGenerator(CodeFormatterConfig()).print(r.sql).stripPrefix("'").stripSuffix("'")
      val text =
        r.sql match
          case s: wvlet.lang.model.expr.StringLiteral =>
            s.unquotedValue
          case other =>
            sql
      parseSqlText(text)

  end DefaultResolver

  private def parseSqlText(text: String): Relation =
    val unit                = CompilationUnit.fromSqlString(text)
    val plan                = SqlParser(unit).parse()
    var q: Option[Relation] = None
    plan.traverseOnce { case r: Relation =>
      if q.isEmpty then
        q = Some(r)
    }
    q.getOrElse(throw StatusCode.SYNTAX_ERROR.newException(s"No query found in: ${text}"))

  private def resolvePath(path: String): String =
    if path.startsWith("/") || path.contains("://") || SourceIO.existsFile(path) then
      path
    else
      val p = s"${baseDir.stripSuffix("/")}/${path}"
      if SourceIO.existsFile(p) then
        p
      else
        path

  /** Build the physical plan of a relation (model references must already be expanded) */
  def physicalPlan(r: Relation)(using ctx: Context): Operator = newPlanner.plan(lowerShow(r))

  /** A planner for one query over this engine's catalog and options */
  private[engine] def newPlanner(using ctx: Context): Planner = Planner(
    catalog,
    DefaultResolver(),
    optimizer,
    accelerator
  )

  /**
    * The engine's runtime plan of a relation (model references must already be expanded), for tests
    * and debugging
    */
  def runtimePlan(r: Relation)(using ctx: Context): wvlet.lang.engine.runtime.RuntimePlan =
    newPlanner.runtimePlan(lowerShow(r))

  /** SHOW statements become their SQL queries over information_schema (or a VALUES list) */
  private def lowerShow(r: Relation)(using ctx: Context): Relation = r
    .transformUp { case s: Show =>
      benchmarkTables(s) match
        case Some(names) =>
          // The built-in tpch/tpcds schemas aren't in information_schema: any scale factor works
          val rows = names.map(n => s"('${n}')").mkString(", ")
          parseSqlText(s"select name from (values ${rows}) as t(name) order by name")
        case None =>
          // e.g., SHOW MODELS lists doc comments joined with ||
          parseSqlText(
            Binder.rewriteConcat(GenSQL.generateSQLFromRelation(s, addHeader = false).sql)
          )
    }
    .asInstanceOf[Relation]

  /**
    * The table names `show tables` lists when it targets a built-in benchmark schema (`tpch.tiny`),
    * given in its `in` clause or by the session's catalog and schema
    */
  private def benchmarkTables(s: Show)(using ctx: Context): Option[Seq[String]] =
    import wvlet.lang.model.expr.{DotRef, Identifier}
    if s.showType != ShowType.tables then
      None
    else
      val opts              = ctx.global.compilerOptions
      val (catalog, schema) =
        s.inExpr match
          case DotRef(q: Identifier, name, _, _) =>
            (Some(q.leafName), Some(name.leafName))
          case i: Identifier if i.nonEmpty =>
            (opts.catalog, Some(i.leafName))
          case _ =>
            (opts.catalog, opts.schema)
      for
        c     <- catalog
        sc    <- schema
        names <- BenchmarkCatalogs.tableNames(c, sc)
      yield names

  private def copy(
      catalog: EngineCatalog = catalog,
      optimizer: OptimizerOptions = optimizer,
      accelerator: Option[PlanAccelerator] = accelerator,
      memoryPool: MemoryPool = memoryPool,
      queryMemoryLimit: Option[Long] = queryMemoryLimit
  ): WvletEngine = WvletEngine(
    catalog,
    baseDir,
    optimizer,
    accelerator,
    memoryPool,
    queryMemoryLimit
  )

  /** An engine running only the given planner optimizations, over the same catalog and folder */
  def withOptimizer(o: OptimizerOptions): WvletEngine = copy(optimizer = o)

  /** An engine over another catalog, resolving files against the same folder */
  def withCatalog(c: EngineCatalog): WvletEngine = copy(catalog = c)

  /** An engine running the plan subtrees `a` supports on it */
  def withAccelerator(a: PlanAccelerator): WvletEngine = copy(accelerator = Some(a))

  /** An engine running every plan on the CPU */
  def noAccelerator(): WvletEngine = copy(accelerator = None)

  /** An engine whose queries reserve memory from `pool` (e.g. one shared by a server's sessions) */
  def withMemoryPool(pool: MemoryPool): WvletEngine = copy(memoryPool = pool)

  /** An engine failing a query that reserves more than `bytes` */
  def withQueryMemoryLimit(bytes: Long): WvletEngine = copy(queryMemoryLimit = Some(bytes))

  /** An engine with no per-query memory limit */
  def noQueryMemoryLimit(): WvletEngine = copy(queryMemoryLimit = None)

  /** The memory context of a new query: counting nothing with `memory_accounting` off */
  private def newQueryMemory(): MemoryContext =
    if optimizer.isEnabled(Optimization.MemoryAccounting) then
      memoryPool.newQuery(queryMemoryLimit.getOrElse(memoryPool.queryLimitBytes))
    else
      MemoryContext.NoOp

  /** Expand model references, plan, and run a query relation */
  def execute(r: Relation)(using ctx: Context): EngineResult =
    // Opened before planning, which runs plan-time subqueries, and closed after the result is
    // drained: the caller owns the result batches from then on
    val memory = newQueryMemory()
    try
      MemoryContext.withCurrent(memory) {
        val expanded = GenSQL.expand(r, ctx)
        val op       = physicalPlan(expanded)
        debug(s"[physical plan]\n${op.explain()}")
        try EngineResult(op.schema, memory.drain(op))
        finally op.close()
      }
    finally memory.close()

  def explain(r: Relation)(using ctx: Context): String = physicalPlan(GenSQL.expand(r, ctx))
    .explain()

  /**
    * Plan a query and return each stage, for tools that show how the engine plans it: the runtime
    * plan as bound, after the logical passes, the rules that fired, and the physical operators. The
    * operators are not run; [[EnginePlanStages.close]] releases them.
    */
  def planStages(r: Relation)(using ctx: Context): EnginePlanStages =
    val planner = Planner(catalog, DefaultResolver(), optimizer, accelerator)
    val op      = planner.plan(lowerShow(GenSQL.expand(r, ctx)))
    EnginePlanStages(
      bound = planner.rootPlans.map(_.bound),
      optimized = planner.rootPlans.map(_.optimized),
      firedRules = planner.firedRules,
      physical = op
    )

end WvletEngine
