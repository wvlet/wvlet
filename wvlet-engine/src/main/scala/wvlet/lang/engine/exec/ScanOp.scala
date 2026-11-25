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
  * reuse cached Parquet footers ([[ParquetFooterCache]]). The source returns only the rows matching
  * `rowFilter` ([[ScanSpec.rowFilter]]), which may read columns outside `columns`.
  *
  * Runtime filters with a per-row test ([[RuntimeFilter.rowFilter]]) also narrow every batch the
  * source returns ([[narrow]]), deferring the other columns' gathers while `lateGather` is on. With
  * `sourceJoinFilters` (`scan_join_row_filters`), the ones expected to keep few rows go into the
  * source's row filter instead ([[joinRowFilters]]), so that a Parquet scan decodes the other
  * columns only for the rows they accept
  */
class ScanOp(
    val source: TableSource,
    val schema: Schema,
    val columns: Option[IndexedSeq[Int]],
    val filter: Option[ParquetFilter],
    val runtimeFilters: Seq[RuntimeFilter] = Nil,
    val footerCache: Boolean = false,
    val rowFilter: Option[ParquetFilter] = None
) extends Operator:
  override def name: String              = s"Scan[${source.name}]"
  override protected def details: String =
    s"(${columns
        .map(c => s"columns=${c.map(source.schema(_).name).mkString(",")}")
        .getOrElse("")}${filter.map(f => s" filter=${f}").getOrElse("")}${rowFilter
        .map(f => s" row_filter=${f}")
        .getOrElse("")}${
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

  /**
    * The scan request this operator sends to the source. Per-row runtime filters set by then may
    * join its row filter ([[joinRowFilters]])
    */
  def spec: ScanSpec =
    val (join, joinPassRate) = joinRowFilters()
    // uni tests the conjuncts in order, each on the rows the ones before it kept
    val conjuncts =
      if joinFiltersFirst && join.nonEmpty && joinPassRate < rowFilterPassRate then
        join ++ rowFilter
      else
        rowFilter ++ join
    ScanSpec(
      columns.map(_.map(source.schema(_).name)),
      (filter ++ runtimeFilters.flatMap(_.filter)).reduceOption(_ && _),
      footerCache,
      conjuncts.reduceOption(_ && _),
      dictionaryCodes
    )

  private lazy val it: Iterator[Batch] = source.scan(spec)

  /** The source column name of output position i */
  def sourceColumnName(i: Int): String = source.schema(columns.map(_(i)).getOrElse(i)).name

  /** Whether [[narrow]] defers the gathers of the columns it doesn't test (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  /**
    * Whether Parquet strings decoded from a dictionary come with their codes, for grouping by them
    * (`dictionary_group_keys`, [[ScanSpec.dictionaryCodes]])
    */
  var dictionaryCodes: Boolean = false

  def withDictionaryCodes(enabled: Boolean): this.type =
    dictionaryCodes = enabled
    this

  /**
    * Whether [[spec]] moves selective per-row runtime filters into the source's row filter
    * (`scan_join_row_filters`)
    */
  var sourceJoinFilters: Boolean = true

  def withSourceJoinFilters(enabled: Boolean): this.type =
    sourceJoinFilters = enabled
    this

  /**
    * Whether [[spec]] puts the per-row runtime filters ahead of [[rowFilter]] in the source's row
    * filter when they are expected to keep fewer rows (`join_row_filters_first`)
    */
  var joinFiltersFirst: Boolean = true

  def withJoinFiltersFirst(enabled: Boolean): this.type =
    joinFiltersFirst = enabled
    this

  /**
    * The estimated fraction of the rows [[rowFilter]] keeps (1.0 when unknown), to order it against
    * the per-row runtime filters in [[spec]]
    */
  var rowFilterPassRate: Double = 1.0

  def withRowFilterPassRate(rate: Double): this.type =
    rowFilterPassRate = rate
    this

  /** Take the flags of `s`, the scan this one rebuilds */
  private def withFlagsOf(s: ScanOp): ScanOp = withLateGather(s.lateGather)
    .withDictionaryCodes(s.dictionaryCodes)
    .withSourceJoinFilters(s.sourceJoinFilters)
    .withJoinFiltersFirst(s.joinFiltersFirst)
    .withRowFilterPassRate(s.rowFilterPassRate)

  def withSchema(s: Schema): ScanOp = ScanOp(
    source,
    s,
    columns,
    filter,
    runtimeFilters,
    footerCache,
    rowFilter
  ).withFlagsOf(this)

  /** Read only the given output positions */
  def project(idx: IndexedSeq[Int]): ScanOp =
    val base = columns.getOrElse(source.schema.fields.indices)
    ScanOp(
      source,
      Schema(idx.map(schema(_))),
      Some(idx.map(base)),
      filter,
      runtimeFilters,
      footerCache,
      rowFilter
    ).withFlagsOf(this)

  def withFilter(f: ParquetFilter): ScanOp = ScanOp(
    source,
    schema,
    columns,
    Some(filter.map(_ && f).getOrElse(f)),
    runtimeFilters,
    footerCache,
    rowFilter
  ).withFlagsOf(this)

  /** Return only the rows matching `f` too (an exact filter, see [[ScanSpec.rowFilter]]) */
  def withRowFilter(f: ParquetFilter): ScanOp = ScanOp(
    source,
    schema,
    columns,
    filter,
    runtimeFilters,
    footerCache,
    Some(rowFilter.map(_ && f).getOrElse(f))
  ).withFlagsOf(this)

  // The filters with a per-row test, each with the output position of its column
  private lazy val rowFilters: Array[(RuntimeFilter, Int)] =
    runtimeFilters
      .filter(_.rows)
      .flatMap(f => schema.fields.indices.find(sourceColumnName(_) == f.column).map(f -> _))
      .toArray

  // Which of `rowFilters` the last spec gave the source, so that narrow doesn't test them again
  @volatile
  private var inSource: Array[Boolean] = null

  /**
    * The per-row runtime filters, set by now, that the source's row filter tests instead of
    * [[narrow]], as `In` conjuncts in ascending order of the fraction of rows they are expected to
    * keep. A filter's fraction comes from the row-group statistics of its column ([[passRate]]),
    * assuming the values spread evenly within each row group. The filters expected to keep at most
    * [[ScanOp.MaxConjunctPassRate]] go in together, and only when all of them are expected to keep
    * at most [[ScanOp.MaxSourcePassRate]] of the rows (multiplying their fractions): uni decodes
    * the first conjunct's column for every row, the next ones for the rows still passing, and the
    * other columns only for the rows passing all, which costs more than decoding everything unless
    * few rows pass (and the source's filter, unlike [[narrow]], doesn't pause). The hash joins
    * still match every key, so a filter that stays in [[narrow]], or is in neither, only keeps more
    * rows. Returns the conjuncts and the fraction of rows they are expected to keep together
    */
  private def joinRowFilters(): (Seq[ParquetFilter], Double) =
    if !sourceJoinFilters || rowFilters.isEmpty || !source.supportsRowFilter then
      return (Nil, 1.0)
    val chosen = rowFilters
      .indices
      .flatMap { i =>
        val (f, pos) = rowFilters(i)
        val rf       = f.rowFilter
        val column   = f.column
        val typed    =
          schema(pos).ptype match
            case PType.Int32 | PType.Int64 | PType.Date =>
              true
            case _ =>
              false
        if rf == null || !typed || !source.canRowFilterOn(column) then
          None
        else
          passRate(column, rf).map((i, rf, _)).filter(_._3 <= ScanOp.MaxConjunctPassRate)
      }
      .sortBy(_._3)
    val kept = chosen.map(_._3).product
    if chosen.isEmpty || kept > ScanOp.MaxSourcePassRate then
      inSource = null
      (Nil, 1.0)
    else
      val flags = new Array[Boolean](rowFilters.length)
      chosen.foreach((i, _, _) => flags(i) = true)
      inSource = flags
      (
        chosen.map((i, rf, _) => ParquetFilter.col(rowFilters(i)._1.column).isIn(rf.integerSet)),
        kept
      )

  end joinRowFilters

  /**
    * The fraction of the rows in the parts (row groups) that `rf`'s range doesn't skip that `rf` is
    * expected to keep: in each part, the share of its [min, max] that `rf` accepts, weighted by its
    * rows. Data clustered by the key (e.g. sales by date) leaves few parts whose rows mostly pass,
    * which the share of the whole column's [min, max] would underestimate. None when the source has
    * no statistics for the column
    */
  private def passRate(column: String, rf: RowKeyFilter): Option[Double] =
    inline def share(lo: Long, hi: Long): Double =
      rf.acceptedIn(lo, hi) / (hi.toDouble - lo.toDouble + 1.0)
    source.partRanges(column) match
      case Some(parts) =>
        val set  = rf.integerSet
        var rows = 0.0
        var kept = 0.0
        parts.foreach { p =>
          if p.max >= set.min && p.min <= set.max then
            rows += p.rows
            kept += p.rows * share(p.min, p.max)
        }
        Some(
          if rows == 0.0 then
            0.0
          else
            kept / rows
        )
      case None =>
        source.columnRange(column).map(share)

  /**
    * The rows of `b` (a batch the source returned) that pass the per-row filters set so far. A
    * filter set after the scan started applies from the next batch. Filters the source's row filter
    * tested ([[joinRowFilters]]) are skipped
    */
  def narrow(b: Batch): Batch =
    if rowFilters.isEmpty || b.size == 0 then
      return b
    var sel: Array[Int] = null
    var n               = b.size
    val skip            = inSource
    var i               = 0
    while i < rowFilters.length do
      val (f, pos) = rowFilters(i)
      val rf       = f.rowFilter
      if rf != null && n > 0 && (skip == null || !skip(i)) && rf.active() then
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

object ScanOp:
  /**
    * Up to which expected fraction of kept rows the per-row runtime filters of a scan go into the
    * source's row filter together ([[ScanOp.joinRowFilters]]). At SF10, store_sales filters keeping
    * 20% cost 13-19% more CPU in the source than after decoding (TPC-DS q47, q57, q70)
    */
  val MaxSourcePassRate: Double = 0.05

  /** Up to which expected fraction of kept rows one per-row runtime filter may join them */
  val MaxConjunctPassRate: Double = 0.5
