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
    val types = schema.types
    rows
      .take(limit)
      .map { r =>
        ListMap.from(names.indices.map(i => names(i) -> ValueOps.toExternal(r(i), types(i))))
      }
      .toSeq

/**
  * The in-process columnar query engine: executes typed wvlet relations on vectors, reading Parquet
  * / Iceberg through uni. Tables are registered in [[catalog]]; file paths in queries are resolved
  * against `baseDir`.
  */
class WvletEngine(val catalog: EngineCatalog = EngineCatalog(), baseDir: String = ".")
    extends LogSupport:

  private class DefaultResolver(using ctx: Context) extends SourceResolver:
    def tableSource(name: String, fullName: String): Option[TableSource] = None

    def fileSource(path: String): Option[TableSource] =
      val resolved = resolvePath(path)
      val lower    = path.toLowerCase
      val name     = path.split("[/\\\\]").last.takeWhile(_ != '.')
      if lower.endsWith(".parquet") || lower.contains("*.parquet") ||
        lower.endsWith(".metadata.json") || SourceIO.isDirectory(resolved)
      then
        Some(ParquetSource(name, resolved))
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
      val unit                = CompilationUnit.fromSqlString(text)
      val plan                = SqlParser(unit).parse()
      var q: Option[Relation] = None
      plan.traverseOnce { case r: Relation =>
        if q.isEmpty then
          q = Some(r)
      }
      q.getOrElse(throw StatusCode.SYNTAX_ERROR.newException(s"No query found in: ${text}"))

  end DefaultResolver

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
  def physicalPlan(r: Relation)(using ctx: Context): Operator = Planner(catalog, DefaultResolver())
    .plan(r)

  /** Expand model references, plan, and run a query relation */
  def execute(r: Relation)(using ctx: Context): EngineResult =
    val expanded = GenSQL.expand(r, ctx)
    val op       = physicalPlan(expanded)
    debug(s"[physical plan]\n${op.explain()}")
    try EngineResult(op.schema, op.drain())
    finally op.close()

  def explain(r: Relation)(using ctx: Context): String = physicalPlan(GenSQL.expand(r, ctx))
    .explain()

end WvletEngine
