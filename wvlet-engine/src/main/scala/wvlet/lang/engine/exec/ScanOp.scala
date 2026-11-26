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
      (filter ++ runtimeFilters.flatMap(_.filter) ++ keySetRowGroupFilters()).reduceOption(_ && _),
      footerCache,
      conjuncts.reduceOption(_ && _),
      dictionaryCodes,
      dictionaryEntries,
      lazyStrings,
      Option(decodeRunner).filter(_ => parallelColumnDecode)
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
    * Whether Parquet strings decoded from a dictionary are read as their entries only, without
    * copying each row's bytes (`dictionary_entry_scans`, [[ScanSpec.dictionaryEntries]])
    */
  var dictionaryEntries: Boolean = false

  def withDictionaryEntries(enabled: Boolean): this.type =
    dictionaryEntries = enabled
    this

  /**
    * Whether Parquet strings decoded from a dictionary are held as their codes and entries,
    * building per-row strings only on first use (`lazy_dictionary_strings`,
    * [[ScanSpec.lazyStrings]])
    */
  var lazyStrings: Boolean = false

  def withLazyStrings(enabled: Boolean): this.type =
    lazyStrings = enabled
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
    * Whether [[passRate]] scales by how sparse the probe column's values are within its range
    * (`sparse_key_pass_rates`)
    */
  var sparseKeyPassRates: Boolean = true

  def withSparseKeyPassRates(enabled: Boolean): this.type =
    sparseKeyPassRates = enabled
    this

  /**
    * Whether [[spec]] lets the source skip the row groups whose [min, max] holds none of a per-row
    * runtime filter's build keys (`key_set_row_groups`, [[keySetRowGroupFilters]])
    */
  var keySetRowGroups: Boolean = true

  def withKeySetRowGroups(enabled: Boolean): this.type =
    keySetRowGroups = enabled
    this

  /**
    * The estimated fraction of the rows [[rowFilter]] keeps (1.0 when unknown), to order it against
    * the per-row runtime filters in [[spec]]
    */
  var rowFilterPassRate: Double = 1.0

  def withRowFilterPassRate(rate: Double): this.type =
    rowFilterPassRate = rate
    this

  /**
    * Whether [[spec]] passes [[decodeRunner]] to the source, so that a scan run as one part decodes
    * its columns in parallel (`parallel_column_decode`)
    */
  var parallelColumnDecode: Boolean = true

  def withParallelColumnDecode(enabled: Boolean): this.type =
    parallelColumnDecode = enabled
    this

  /**
    * The runner of the pipeline running this scan as a single part ([[MorselOp]]), set before the
    * scan starts; null otherwise
    */
  var decodeRunner: TaskRunner = null

  def withDecodeRunner(runner: TaskRunner): this.type =
    decodeRunner = runner
    this

  /** Take the flags of `s`, the scan this one rebuilds */
  private def withFlagsOf(s: ScanOp): ScanOp = withLateGather(s.lateGather)
    .withDictionaryCodes(s.dictionaryCodes)
    .withDictionaryEntries(s.dictionaryEntries)
    .withLazyStrings(s.lazyStrings)
    .withSourceJoinFilters(s.sourceJoinFilters)
    .withJoinFiltersFirst(s.joinFiltersFirst)
    .withSparseKeyPassRates(s.sparseKeyPassRates)
    .withKeySetRowGroups(s.keySetRowGroups)
    .withRowFilterPassRate(s.rowFilterPassRate)
    .withParallelColumnDecode(s.parallelColumnDecode)

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

  /**
    * Return only the rows matching `f` too (an exact filter, see [[ScanSpec.rowFilter]]), which is
    * estimated to keep `passRate` of them ([[rowFilterPassRate]])
    */
  def withRowFilter(f: ParquetFilter, passRate: Double = 1.0): ScanOp = ScanOp(
    source,
    schema,
    columns,
    filter,
    runtimeFilters,
    footerCache,
    Some(rowFilter.map(_ && f).getOrElse(f))
  ).withFlagsOf(this).withRowFilterPassRate(rowFilterPassRate * passRate)

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
    * rows. With `sparseKeyPassRates`, the conjuncts are ordered by fractions that count a sparse
    * column's distinct values ([[sparseness]]), while the limits above still apply to the fractions
    * over the column's range width, which they were tuned with. Returns the conjuncts and the
    * fraction of rows they are expected to keep together, by the fractions they are ordered by
    */
  private def joinRowFilters(): (Seq[ParquetFilter], Double) =
    if !sourceJoinFilters || rowFilters.isEmpty || !source.supportsRowFilter then
      return (Nil, 1.0)
    val chosen = rowFilters
      .indices
      .flatMap { i =>
        val (f, pos) = rowFilters(i)
        val column   = f.column
        sourceTestable(f, pos).flatMap { rf =>
          passRate(column, rf, sparse = false)
            .filter(_ <= ScanOp.MaxConjunctPassRate)
            .map { rate =>
              val order =
                if sparseKeyPassRates then
                  passRate(column, rf, sparse = true).getOrElse(rate)
                else
                  rate
              (i, rf, rate, order)
            }
        }
      }
      .sortBy(_._4)
    val kept = chosen.map(_._3).product
    if chosen.isEmpty || kept > ScanOp.MaxSourcePassRate then
      inSource = null
      (Nil, 1.0)
    else
      val flags = new Array[Boolean](rowFilters.length)
      chosen.foreach((i, _, _, _) => flags(i) = true)
      inSource = flags
      (
        chosen.map((i, rf, _, _) => ParquetFilter.col(rowFilters(i)._1.column).isIn(rf.integerSet)),
        chosen.map(_._4).product
      )

  end joinRowFilters

  /**
    * Row-group filters (`In` over [[RowKeyFilter.prunedSet]]) for the per-row runtime filters set
    * by now whose build keys skip more row groups than their [min, max] range: the source skips a
    * row group whose statistics hold no build key, without testing rows (a source may ignore them).
    * A few build keys on a probe column the file is clustered by, such as TPC-DS store_sales'
    * ticket numbers matching a handful of returns (q17, q25), leave most row groups unread
    */
  private def keySetRowGroupFilters(): Seq[ParquetFilter] =
    if !keySetRowGroups || rowFilters.isEmpty || !source.supportsKeySetRowGroups then
      return Nil
    rowFilters
      .toSeq
      .flatMap { (f, pos) =>
        sourceTestable(f, pos)
          .filter(_.prunesBeyondRange)
          .map(rf => ParquetFilter.col(f.column).isIn(rf.prunedSet))
      }

  /**
    * The per-row filter of `f` (over output position `pos`) once set, when the source can test it:
    * an integer or date column the source's row filter compares as the engine does
    */
  private def sourceTestable(f: RuntimeFilter, pos: Int): Option[RowKeyFilter] =
    val rf    = f.rowFilter
    val typed =
      schema(pos).ptype match
        case PType.Int32 | PType.Int64 | PType.Date =>
          true
        case _ =>
          false
    Option(rf).filter(_ => typed && source.canRowFilterOn(f.column))

  /**
    * Whether the source reads a part with [min, max] of `rf`'s column ([[keySetRowGroupFilters]])
    */
  private def partRead(rf: RowKeyFilter, min: Long, max: Long): Boolean =
    if keySetRowGroups && rf.prunesBeyondRange then
      rf.mayIntersect(min, max)
    else
      max >= rf.integerSet.min && min <= rf.integerSet.max

  /**
    * The fraction of the rows in the parts (row groups) the source reads ([[partRead]]) that `rf`
    * is expected to keep: in each part, the share of its [min, max] that `rf` accepts, weighted by
    * its rows. Data clustered by the key (e.g. sales by date) leaves few parts whose rows mostly
    * pass, which the share of the whole column's [min, max] would underestimate. None when the
    * source has no statistics for the column. With `sparse`, the build keys' share counts the
    * column's distinct values ([[sparseness]]); a range filter keeps every value of its range, so
    * its share is exact either way
    */
  private def passRate(column: String, rf: RowKeyFilter, sparse: Boolean): Option[Double] =
    val density =
      if sparse && rf.kind != RowKeyFilter.Kind.Range then
        sparseness(column)
      else
        1.0
    // A Bloom filter's false positives are a share of the probe values themselves: only the build
    // keys' share scales
    val falsePositives =
      if rf.kind == RowKeyFilter.Kind.Bloom then
        RowKeyFilter.BloomFalsePositives
      else
        0.0
    inline def share(lo: Long, hi: Long): Double =
      val accepted = rf.acceptedIn(lo, hi) / (hi.toDouble - lo.toDouble + 1.0)
      if density == 1.0 then
        accepted
      else
        math.min(
          1.0,
          math.max(0.0, accepted - falsePositives) * density + math.min(accepted, falsePositives)
        )
    source.partRanges(column) match
      case Some(parts) =>
        var rows = 0.0
        var kept = 0.0
        parts.foreach { p =>
          if partRead(rf, p.min, p.max) then
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

  end passRate

  /**
    * How many values of the range of `column` there are per distinct value it holds (at least 1.0;
    * 1.0 when unknown): the build keys of a join filter are values of the probe column, so the
    * share of the probe rows they keep is their count over the column's distinct values in the
    * range, not over its width. A column whose equal values come in runs (lineitem's order keys,
    * about 4 lines per order) has at most its rows over the mean run length distinct values, which
    * metadata-only distinct estimates (at most the range width) miss
    */
  private def sparseness(column: String): Double =
    val r =
      for
        (lo, hi) <- source.columnRange(column)
        d        <- source.estimatedDistinct(column)
      yield
        val runs =
          for
            rows <- source.estimatedRows
            run  <- source.keyRunLength(column)
            if run > 1.0
          yield rows.toDouble / run
        val distinct = runs.fold(d)(math.min(d, _))
        (hi.toDouble - lo.toDouble + 1.0) / math.max(1.0, distinct)
    r.map(math.max(1.0, _)).getOrElse(1.0)

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
