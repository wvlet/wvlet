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
package wvlet.lang.engine.exec

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetFilter

/**
  * Reads a table source. `schema` is the output schema (source columns, possibly renamed or
  * requalified); `columns` are the source column positions to read (all when None); `filter` lets
  * the source skip data that can't match (see [[ScanSpec]]), as do the `runtimeFilters` that hash
  * joins above set once their builds are done. With `footerCache`, the scan asks the source to
  * reuse cached Parquet footers ([[ParquetFooterCache]]).
  *
  * Runtime filters with a per-row test ([[RuntimeFilter.rowFilter]]) also narrow every batch the
  * source returns ([[narrow]]), deferring the other columns' gathers while `lateGather` is on
  */
class ScanOp(
    val source: TableSource,
    val schema: Schema,
    val columns: Option[IndexedSeq[Int]],
    val filter: Option[ParquetFilter],
    val runtimeFilters: Seq[RuntimeFilter] = Nil,
    val footerCache: Boolean = false
) extends Operator:
  override def name: String              = s"Scan[${source.name}]"
  override protected def details: String =
    s"(${columns
        .map(c => s"columns=${c.map(source.schema(_).name).mkString(",")}")
        .getOrElse("")}${filter.map(f => s" filter=${f}").getOrElse("")}${
        if runtimeFilters.isEmpty then
          ""
        else
          s" join_filter=${runtimeFilters.map(_.column).distinct.mkString(",")}"
      })"

  override protected def extraInputs(indent: Int): Seq[String] = source.explainDetail(indent).toSeq

  // A breaker's plan is not a child (the source drains and closes it), but walks must reach it
  override def sharedInputs: Seq[Operator] =
    source match
      case b: BreakerSource =>
        Seq(b.op)
      case _ =>
        Nil

  /** The scan request this operator sends to the source */
  def spec: ScanSpec = ScanSpec(
    columns.map(_.map(source.schema(_).name)),
    (filter ++ runtimeFilters.flatMap(_.filter)).reduceOption(_ && _),
    footerCache
  )

  private lazy val it: Iterator[Batch] = source.scan(spec)

  /** The source column name of output position i */
  def sourceColumnName(i: Int): String = source.schema(columns.map(_(i)).getOrElse(i)).name

  /** Whether [[narrow]] defers the gathers of the columns it doesn't test (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  def withSchema(s: Schema): ScanOp = ScanOp(
    source,
    s,
    columns,
    filter,
    runtimeFilters,
    footerCache
  ).withLateGather(lateGather)

  /** Read only the given output positions */
  def project(idx: IndexedSeq[Int]): ScanOp =
    val base = columns.getOrElse(source.schema.fields.indices)
    ScanOp(
      source,
      Schema(idx.map(schema(_))),
      Some(idx.map(base)),
      filter,
      runtimeFilters,
      footerCache
    ).withLateGather(lateGather)

  def withFilter(f: ParquetFilter): ScanOp = ScanOp(
    source,
    schema,
    columns,
    Some(filter.map(_ && f).getOrElse(f)),
    runtimeFilters,
    footerCache
  ).withLateGather(lateGather)

  // The filters with a per-row test, each with the output position of its column
  private lazy val rowFilters: Array[(RuntimeFilter, Int)] =
    runtimeFilters
      .filter(_.rows)
      .flatMap(f => schema.fields.indices.find(sourceColumnName(_) == f.column).map(f -> _))
      .toArray

  /**
    * The rows of `b` (a batch the source returned) that pass the per-row filters set so far. A
    * filter set after the scan started applies from the next batch
    */
  def narrow(b: Batch): Batch =
    if rowFilters.isEmpty || b.size == 0 then
      return b
    var sel: Array[Int] = null
    var n               = b.size
    var i               = 0
    while i < rowFilters.length do
      val (f, pos) = rowFilters(i)
      val rf       = f.rowFilter
      if rf != null && n > 0 && rf.active() then
        val out =
          if sel == null then
            new Array[Int](n)
          else
            sel
        val kept = rf.select(b.column(pos), sel, n, out)
        rf.record(n, kept)
        sel = out
        n = kept
      i += 1
    if sel == null || n == b.size then
      b
    else if lateGather then
      b.take(sel, n)
    else
      b.gather(sel, n)

  end narrow

  def next(): Batch =
    var b: Batch = null
    while b == null && it.hasNext do
      val x = narrow(it.next())
      if x.size > 0 then
        b = x
    b

end ScanOp
