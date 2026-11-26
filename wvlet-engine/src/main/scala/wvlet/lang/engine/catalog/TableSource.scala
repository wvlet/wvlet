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
package wvlet.lang.engine.catalog

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.*
import wvlet.uni.cloud.parquet.format.LogicalType
import wvlet.uni.cloud.parquet.format.PhysicalType
import wvlet.uni.cloud.parquet.format.RowGroupMeta
import wvlet.uni.cloud.parquet.format.TimeUnit

import scala.collection.mutable

/**
  * Scan request pushed down to a table source. `filter` is implied by the query's own filter: a
  * source may use it to skip data that can't match (e.g. Parquet row groups by statistics), but
  * rows it returns are still filtered by the engine. `rowFilter` is exact: the source returns only
  * the rows it matches, and the engine doesn't evaluate it again. Only a source that
  * [[TableSource.supportsRowFilter]] gets one; its columns may be outside `columns`. With
  * `dictionaryCodes`, Parquet strings decoded from a dictionary carry their codes
  * ([[StrVec.codes]]). With `dictionaryEntries`, Parquet sources read such strings as their
  * dictionary entries only, without copying each row's bytes. With `lazyStrings` (and
  * `dictionaryCodes`), such vectors hold only the codes and their entries' strings, building the
  * per-row strings on first use ([[StrVec.ofEntries]]). With `decodeRunner`, a Parquet scan that
  * reads a single row group without a row filter decodes its columns as tasks of that runner
  * (`parallel_column_decode`)
  */
case class ScanSpec(
    columns: Option[Seq[String]] = None,
    filter: Option[ParquetFilter] = None,
    footerCache: Boolean = false,
    rowFilter: Option[ParquetFilter] = None,
    dictionaryCodes: Boolean = false,
    dictionaryEntries: Boolean = false,
    lazyStrings: Boolean = false,
    decodeRunner: Option[TaskRunner] = None
)

/** A readable table: its columns and a batch iterator over the requested columns */
trait TableSource:
  def name: String
  def schema: Schema

  /** Batches with exactly the requested columns (all columns when not given), in that order */
  def scan(spec: ScanSpec): Iterator[Batch]

  /** Whether `scan` uses `spec.filter` to skip data; otherwise the filter is ignored */
  def supportsFilter: Boolean = false

  /** Whether `spec.filter` may refer to this column */
  def canFilterOn(column: String): Boolean = supportsFilter

  /**
    * Whether `scan` returns only the rows matching `spec.rowFilter`, decoding the other columns
    * only for those rows. A source without it never gets a row filter
    */
  def supportsRowFilter: Boolean = false

  /** Whether `spec.rowFilter` may compare this column, exactly as the engine compares its values */
  def canRowFilterOn(column: String): Boolean = false

  /**
    * The [min, max] range of an integer (or date, in epoch days) column's non-null values, from
    * metadata only, for estimating how many rows a range condition keeps. None when unknown
    */
  def columnRange(column: String): Option[(Long, Long)] = None

  /**
    * The [min, max] range of an integer (or date) column's non-null values in each part of the
    * source (Parquet row groups) holding any, with the part's row count, from metadata only. None
    * when unknown
    */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] = None

  /** Row count estimate for join ordering */
  def estimatedRows: Option[Long] = None

  /**
    * Estimate of the number of distinct non-null values of a column, for join ordering. None when
    * unknown
    */
  def estimatedDistinct(column: String): Option[Double] = None

  /**
    * The distinct values of a string column in a sample of its rows, with their counts, for
    * estimating how many rows a condition over the column keeps. None when unknown
    */
  def valueCounts(column: String): Option[ValueCounts] = None

  /**
    * The mean number of consecutive rows sharing a column's value (NULL counting as a value) in a
    * sample of the rows in storage order, for estimating the groups of filtered rows: the lines of
    * an order share its customer and date, so a filter keeps or drops them together. None when
    * unknown
    */
  def keyRunLength(column: String): Option[Double] = None

  /**
    * Column sets declared unique (primary keys included), by column name: no two rows share the
    * values of all of a set's columns. Join estimates read them, so a source that declares none of
    * its keys is estimated as before; a source declaring some is taken to declare all of them
    */
  def uniqueKeys: Seq[Seq[String]] = Nil

  /** Foreign keys declared for this source (recorded; join estimates don't read them yet) */
  def foreignKeys: Seq[ForeignKey] = Nil

  /** EXPLAIN lines of what a scan of this source reads, nested above the scan (at `indent`) */
  def explainDetail(indent: Int): Option[String] = None

  /** Whether [[scanSplits]] may cut a scan into parts */
  def canSplit: Boolean = false

  /**
    * The scan cut into parts that can be read independently and concurrently (e.g. Parquet row
    * groups), whose batches in part order are the batches of `scan(spec)`. None when the source
    * can't split, or the scan has a single part
    */
  def scanSplits(spec: ScanSpec): Option[ScanSplits] = None

  /**
    * The identity of the data this source reads: sources with equal keys read the same rows, so
    * that plans over them can be shared. The source itself by default; sources created per
    * reference of the same file use their path
    */
  def key: Any = this

end TableSource

/**
  * Independently readable parts of a scan; `close` releases what the parts share. `rows` holds the
  * row count of each part when the source knows it (e.g. from Parquet row-group metadata), and is
  * empty otherwise
  */
case class ScanSplits(
    parts: IndexedSeq[() => Iterator[Batch]],
    close: () => Unit,
    rows: IndexedSeq[Long] = IndexedSeq.empty
)

/** The [min, max] range of a column's non-null values in a part of a source, holding `rows` rows */
case class ColumnPartRange(min: Long, max: Long, rows: Long)

/** An in-memory table */
class MemTable(
    val name: String,
    val schema: Schema,
    batches: Seq[Batch],
    override val uniqueKeys: Seq[Seq[String]] = Nil
) extends TableSource:
  override def estimatedRows: Option[Long]  = Some(batches.map(_.size.toLong).sum)
  def scan(spec: ScanSpec): Iterator[Batch] =
    spec.columns match
      case None =>
        batches.iterator
      case Some(cols) =>
        val idx = cols.map(c => schema.names.indexOf(c)).toArray
        batches.iterator.map(_.select(idx))

/**
  * A Parquet file, a directory of Parquet files (Hive partitions), or an Iceberg table. With
  * `footerCache`, planning reads (schema, row count, statistics) of a single file open it through
  * [[ParquetFooterCache]]; scans do when their [[ScanSpec]] says so
  */
class ParquetSource(
    val name: String,
    val path: String,
    val footerCache: Boolean = false,
    declaredKeys: Seq[Seq[String]] = Nil
) extends TableSource:
  override def key: Any = ("parquet", path)

  // Keys of a sidecar file (`<name>.keys.json`, or `_keys.json` in a folder), read when first asked
  private lazy val sidecarKeys: TableKeys = TableKeys.forPath(path).over(schema.names)

  /** The keys given at registration, or else those of the data's sidecar key file */
  override def uniqueKeys: Seq[Seq[String]] =
    if declaredKeys.nonEmpty then
      declaredKeys
    else
      sidecarKeys.uniqueKeys

  override def foreignKeys: Seq[ForeignKey] = sidecarKeys.foreignKeys

  /** Whether `path` names one Parquet file, rather than a folder or a glob */
  val isSingleFile: Boolean = path.endsWith(".parquet") && !path.exists(c => c == '*' || c == '{')

  /** Open the single file, through the footer cache when `cached` */
  private def openFile(cached: Boolean): ParquetFooterCache.Opened =
    if cached then
      ParquetFooterCache.open(path)
    else
      ParquetFooterCache.Opened(Parquet.open(path), None, false)

  private lazy val parquetSchema: ParquetSchema =
    if isSingleFile then
      val f = openFile(footerCache).file
      try f.schema
      finally f.close()
    else
      val ds = ParquetSource.openDataset(path)
      try ds.schema
      finally ds.close()

  lazy val schema: Schema = Schema(
    parquetSchema.fields.map(f => Field(f.name, ParquetSource.ptypeOf(f), List(name)))
  )

  override def supportsFilter: Boolean = true

  /**
    * UINT64 columns are read as signed Int64 values while Parquet statistics order them unsigned,
    * so a filter on them could skip row groups holding matching rows
    */
  override def canFilterOn(column: String): Boolean = parquetSchema
    .field(column)
    .forall {
      case p: ParquetField.Primitive =>
        !(
          p.physicalType == PhysicalType.INT64 &&
            p.logicalType.contains(LogicalType.Integer(64, false))
        )
      case _ =>
        true
    }

  /** Single files only: a folder or Iceberg table still filters rows after decoding them */
  override def supportsRowFilter: Boolean = isSingleFile

  /**
    * Top-level primitive columns that uni's row filter compares as the engine compares their
    * values: signed integers, dates, booleans, MILLIS or MICROS timestamps (the engine floors NANOS
    * to micros, while uni would compare the nanos), and strings
    */
  override def canRowFilterOn(column: String): Boolean = parquetSchema
    .fields
    .find(_.name == column)
    .exists {
      case p: ParquetField.Primitive if !p.isRepeated =>
        (p.physicalType, p.logicalType) match
          case (PhysicalType.BOOLEAN, None) =>
            true
          case (PhysicalType.INT32 | PhysicalType.INT64, None) =>
            true
          case (PhysicalType.INT32 | PhysicalType.INT64, Some(LogicalType.Integer(_, true))) =>
            true
          case (PhysicalType.INT32, Some(LogicalType.Date)) =>
            true
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(_, unit))) =>
            unit == TimeUnit.MILLIS || unit == TimeUnit.MICROS
          case (PhysicalType.BYTE_ARRAY, None | Some(LogicalType.String | LogicalType.Enum)) =>
            true
          case _ =>
            false
      case _ =>
        false
    }

  // Column statistics of the single file, summed over its row groups, read once per source
  private lazy val fileStatistics: Map[String, ColumnStatistics] =
    try
      val opened = openFile(footerCache)
      try opened.columnStatistics
      finally opened.file.close()
    catch
      case _: Exception =>
        Map.empty

  /**
    * From the file's column statistics, for signed integer and date columns (canFilterOn excludes
    * UINT64, whose statistics are ordered unsigned)
    */
  override def columnRange(column: String): Option[(Long, Long)] =
    def asLong(v: Any): Option[Long] =
      v match
        case i: Int =>
          Some(i.toLong)
        case l: Long =>
          Some(l)
        case d: java.time.LocalDate =>
          Some(d.toEpochDay)
        case _ =>
          None
    schema.fields.find(_.name == column).map(_.ptype) match
      case Some(PType.Int32 | PType.Int64 | PType.Date) if isSingleFile && canFilterOn(column) =>
        for
          s   <- fileStatistics.get(column)
          min <- s.min.flatMap(asLong)
          max <- s.max.flatMap(asLong)
        yield (min, max)
      case _ =>
        None

  // The row groups of the single file, read once per source
  private lazy val rowGroupMeta: IndexedSeq[format.RowGroupMeta] =
    try
      val opened = openFile(footerCache)
      try opened.file.rowGroups
      finally opened.file.close()
    catch
      case _: Exception =>
        IndexedSeq.empty

  /**
    * From the statistics of each row group, for the columns [[columnRange]] covers. Row groups
    * holding only nulls are left out; None when any other row group lacks statistics
    */
  override def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] =
    if columnRange(column).isEmpty then
      return None
    parquetSchema.field(column) match
      case Some(p: ParquetField.Primitive) if rowGroupMeta.nonEmpty =>
        val n =
          p.physicalType match
            case PhysicalType.INT32 =>
              4
            case _ =>
              8
        val ranges = rowGroupMeta.map { rg =>
          rg.columns
            .lift(p.columnIndex)
            .flatMap { c =>
              c.statistics
                .flatMap { s =>
                  if s.nullCount.contains(c.numValues) then
                    Some(None)
                  else
                    for
                      min <- s.min
                      if min.length == n
                      max <- s.max
                      if max.length == n
                    yield Some(
                      ColumnPartRange(
                        DecimalStats.littleEndian(min, n),
                        DecimalStats.littleEndian(max, n),
                        rg.numRows
                      )
                    )
                }
            }
        }
        if ranges.exists(_.isEmpty) then
          None
        else
          Some(ranges.flatMap(_.get))
      case _ =>
        None
    end match

  end partRanges

  override lazy val estimatedRows: Option[Long] =
    try
      if isSingleFile then
        val f = openFile(footerCache).file
        try Some(f.numRows)
        finally f.close()
      else
        None
    catch
      case _: Exception =>
        None

  /**
    * Integer and date columns: the width of the [min, max] range in the file statistics, at most
    * the non-null count. Strings of files up to [[ParquetSource.DistinctScanLimit]] rows: counted
    * by reading the column once, as DuckDB keeps distinct-count statistics for join ordering.
    * Estimates are cached per file version (see [[ParquetSource.MaxDistinctEntries]]); a file
    * [[ParquetFooterCache]] wouldn't cache is estimated once per source (a source lives for one
    * query)
    */
  override def estimatedDistinct(column: String): Option[Double] =
    cachedPerVersion(column, ParquetSource.DistinctStat)(computeDistinct(column))

  /**
    * Strings of a single file: counted in up to [[ParquetSource.ValueSampleRowGroups]] row groups
    * spread over the file, until [[ParquetSource.ValueSampleRows]] rows, and per Parquet dictionary
    * code where the pages are dictionary-encoded. None for a column with more than
    * [[ParquetSource.MaxSampleValues]] distinct values in the sample. Cached per file version as
    * [[estimatedDistinct]] is
    */
  override def valueCounts(column: String): Option[ValueCounts] =
    if !isSingleFile || !schema.fields.exists(f => f.name == column && f.ptype == PType.Str) then
      None
    else
      cachedPerVersion(column, ParquetSource.ValueCountsStat)(computeValueCounts(column))

  /**
    * Integer, date and string columns of a single file: measured in the row groups that
    * [[valueCounts]] samples. Cached per file version as [[estimatedDistinct]] is
    */
  override def keyRunLength(column: String): Option[Double] =
    if !isSingleFile ||
      !schema
        .fields
        .exists(f =>
          f.name == column && (
            f.ptype == PType.Int32 || f.ptype == PType.Int64 || f.ptype == PType.Date ||
              f.ptype == PType.Str
          )
        )
    then
      None
    else
      cachedPerVersion(column, ParquetSource.RunLengthStat)(computeRunLength(column))

  /**
    * A statistic `stat` of `column`, computed once per file version and shared by the sources of
    * the file (an LRU per statistic, [[ParquetSource.MaxDistinctEntries]] distinct estimates and
    * [[ParquetSource.MaxValueCountsEntries]] value samples); a file [[ParquetFooterCache]] wouldn't
    * cache is computed once per source (a source lives for one query)
    */
  private def cachedPerVersion[A](column: String, stat: String)(compute: => Option[A]): Option[A] =
    if !isSingleFile then
      None
    else
      fileVersion() match
        case None =>
          // A file whose rewrites can't be told apart: not shared with other queries
          localStats
            .synchronized(localStats.getOrElseUpdate((column, stat), compute))
            .asInstanceOf[Option[A]]
        case Some(v) =>
          ParquetSource.cachedStat(path, column, stat, v) match
            case Some(d) =>
              d.asInstanceOf[Option[A]]
            case None =>
              // Computed outside the lock, so that a scan doesn't block other planners
              val d = compute
              ParquetSource.putStat(path, column, stat, v, d)
              d

  // The file version (None: not trusted) and when it was read (System.nanoTime)
  @volatile
  private var checkedVersion: (Long, Option[(Long, Long)]) = null

  /**
    * The version (size, last-modified) of the file, read again at most every
    * [[ParquetSource.VersionRecheckNanos]]: join ordering asks for estimates thousands of times per
    * query (TPC-DS q64, 18 joins), and a stat each time took most of the planning time. Read before
    * computing an estimate, so that the entry describes the file read or an older version
    */
  private def fileVersion(): Option[(Long, Long)] =
    val now     = System.nanoTime()
    val checked = checkedVersion
    if checked != null && now - checked._1 < ParquetSource.VersionRecheckNanos then
      checked._2
    else
      val v = ParquetFooterCache
        .fileVersion(path)
        .filterNot((_, mtime) => ParquetFooterCache.isRecentWrite(mtime))
      checkedVersion = (now, v)
      v

  // Estimates of a file without a trusted version, kept for this source only
  private val localStats = scala.collection.mutable.HashMap.empty[(String, String), Option[Any]]

  private def computeValueCounts(column: String): Option[ValueCounts] =
    try
      val f = openFile(footerCache).file
      try ValueCounts.count(sampleBatches(f, column), ParquetSource.MaxSampleValues)
      finally f.close()
    catch
      case _: Exception =>
        None

  private def computeRunLength(column: String): Option[Double] =
    try
      val f = openFile(footerCache).file
      try
        var rows = 0L
        var runs = 0L
        sampleBatches(f, column).foreach { b =>
          if b.size > 0 then
            // A run starts at each row group's first row: the sampled groups aren't adjacent
            val v = b.column(0)
            rows += b.size
            runs += 1
            var i = 1
            while i < b.size do
              if v.isNull(i) != v.isNull(i - 1) || !v.isNull(i) && v.get(i) != v.get(i - 1) then
                runs += 1
              i += 1
        }
        Option.when(runs > 0)(rows.toDouble / runs)
      finally
        f.close()
    catch
      case _: Exception =>
        None

  /**
    * Column `column` of up to [[ParquetSource.ValueSampleRowGroups]] row groups spread over the
    * file, until [[ParquetSource.ValueSampleRows]] rows, string pages as dictionary codes where
    * they are dictionary-encoded
    */
  private def sampleBatches(f: ParquetFile, column: String): Iterator[Batch] =
    val groups  = f.rowGroups
    val n       = groups.size
    val samples = math.min(n, ParquetSource.ValueSampleRowGroups)
    // Spread over the file: generated and appended data is often ordered by a key
    val picked                 = (0 until samples).map(i => (i.toLong * n / samples).toInt).distinct
    val (cols, types, options) = readOptions(ScanSpec(Some(Seq(column))))
    var sampled                = 0L
    picked
      .iterator
      .takeWhile(_ => sampled < ParquetSource.ValueSampleRows)
      .map { g =>
        sampled += groups(g).numRows
        ParquetSource.toBatch(f.readRowGroup(g, options), cols, types, dictionaryCodes = true)
      }

  private def computeDistinct(column: String): Option[Double] =
    if !isSingleFile then
      None
    else
      try
        val opened = openFile(footerCache)
        val f      = opened.file
        try
          val rows    = f.numRows
          val stats   = opened.columnStatistics.get(column)
          val nonNull = stats.flatMap(_.numNulls).map(n => rows - n).getOrElse(rows).toDouble
          schema.fields.find(_.name == column).map(_.ptype) match
            case Some(PType.Int32 | PType.Int64 | PType.Date) =>
              columnRange(column).map((min, max) =>
                math.min(nonNull, max.toDouble - min.toDouble + 1)
              )
            case Some(PType.Str) if rows <= ParquetSource.DistinctScanLimit =>
              val seen = scala.collection.mutable.HashSet.empty[Any]
              scan(ScanSpec(Some(Seq(column)), footerCache = footerCache)).foreach { b =>
                val v = b.columns(0)
                var i = 0
                while i < b.size do
                  val x = v.get(i)
                  if x != null then
                    seen += x
                  i += 1
              }
              Some(math.max(1.0, seen.size.toDouble))
            case _ =>
              None
        finally
          f.close()
        end try
      catch
        case _: Exception =>
          None

  private def readOptions(spec: ScanSpec): (Seq[String], Seq[PType], ParquetReadOptions) =
    ParquetSource.readOptions(schema, spec)

  override def canSplit: Boolean = isSingleFile

  /** One part per row group the filter doesn't skip */
  override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    if !isSingleFile then
      None
    else
      val (cols, types, options) = readOptions(spec)
      val f                      = openFile(spec.footerCache).file
      val groups                 =
        try
          ParquetSource.selectRowGroups(f, spec, options)
        catch
          case e: Throwable =>
            f.close()
            throw e
      if groups.size <= 1 then
        f.close()
        None
      else
        val parts = groups.map { g => () =>
          Iterator.single(
            ParquetSource.toBatch(
              f.readRowGroup(g, options),
              cols,
              types,
              spec.dictionaryCodes,
              spec.lazyStrings
            )
          )
        }
        val meta = f.rowGroups
        Some(ScanSplits(parts, () => f.close(), groups.map(g => meta(g).numRows)))

  def scan(spec: ScanSpec): Iterator[Batch] =
    val (cols, types, options)       = readOptions(spec)
    def batch(b: ColumnBatch): Batch = ParquetSource.toBatch(
      b,
      cols,
      types,
      spec.dictionaryCodes,
      spec.lazyStrings
    )
    if isSingleFile then
      val f                   = openFile(spec.footerCache).file
      val it: Iterator[Batch] =
        try
          parallelDecode(f, spec, cols, options) match
            case Some(b) =>
              Iterator.single(b)
            case None if spec.filter.exists(DecimalStats.hasDecimalCompare(_, f.schema)) =>
              // Read only the row groups the decimal statistics don't skip either
              ParquetSource
                .selectRowGroups(f, spec, options)
                .iterator
                .map(g => batch(f.readRowGroup(g, options)))
            case None =>
              f.readBatches(options).map(batch)
        catch
          case e: Throwable =>
            f.close()
            throw e
      ParquetSource.closing(it, () => f.close())
    else
      val ds = ParquetSource.openDataset(path)
      ParquetSource.closing(ds.readBatches(options).map(batch), () => ds.close())
    end if

  end scan

  /**
    * The only row group `f` reads for `spec`, decoded by tasks of `spec.decodeRunner`
    * (`parallel_column_decode`) when there is no row filter and its columns make more than one task
    * ([[ParquetSource.decodeTasks]]); None to read it as before
    */
  private def parallelDecode(
      f: ParquetFile,
      spec: ScanSpec,
      cols: Seq[String],
      options: ParquetReadOptions
  ): Option[Batch] = spec
    .decodeRunner
    .filter(r => r.parallelism > 1 && cols.size > 1 && spec.rowFilter.isEmpty)
    .flatMap { runner =>
      val groups = ParquetSource.selectRowGroups(f, spec, options)
      if groups.size == 1 then
        val tasks = ParquetSource.decodeTasks(f.rowGroups(groups.head), cols)
        Option.when(tasks.size > 1)(
          readColumnsInParallel(f, groups.head, spec, cols, tasks, runner)
        )
      else
        None
    }

  /**
    * Row group `g` of `f` read with one task of `runner` per group of column positions in `tasks`
    * ([[ParquetSource.decodeTasks]], `parallel_column_decode`): each task decompresses and decodes
    * its column chunks and builds their engine vectors, the calling thread taking the first. Only
    * for a read without a row filter, so that every column holds the same rows
    */
  private def readColumnsInParallel(
      f: ParquetFile,
      g: Int,
      spec: ScanSpec,
      cols: Seq[String],
      tasks: IndexedSeq[IndexedSeq[Int]],
      runner: TaskRunner
  ): Batch =
    val vecs = new Array[Vec](cols.size)
    TaskRunner.inParallel(runner, tasks.size) { t =>
      val positions          = tasks(t)
      val (names, types, op) = readOptions(spec.copy(columns = Some(positions.map(cols))))
      val b                  = f.readRowGroup(g, op)
      positions
        .indices
        .foreach { k =>
          vecs(positions(k)) = ParquetSource.toVec(
            b.column(names(k)),
            types(k),
            spec.dictionaryCodes,
            spec.lazyStrings
          )
        }
    }
    Batch(vecs, f.rowGroups(g).numRows.toInt)

end ParquetSource

object ParquetSource:

  /**
    * At least how many uncompressed bytes of column chunks a task decoding part of a single row
    * group takes (`parallel_column_decode`): a smaller task costs more than it saves. TPC-DS q06
    * read item's three small columns (~1.5 MB at SF10) with 8% more CPU as one task per column
    */
  val ParallelDecodeMinBytes: Long = 2L * 1024 * 1024

  /**
    * The positions of `cols` grouped into the tasks decoding row group `meta` in parallel, largest
    * first: a column of at least [[ParallelDecodeMinBytes]] (uncompressed) is a task of its own,
    * and smaller ones share tasks of about that size, the last joining the one before when it is
    * smaller. One task (or none) means decoding on one thread
    */
  def decodeTasks(meta: RowGroupMeta, cols: Seq[String]): IndexedSeq[IndexedSeq[Int]] =
    // A nested column's leaves all count toward its top-level name
    val sizes = meta.columns.groupMapReduce(_.path.head)(_.totalUncompressedSize)(_ + _)
    val bytes = cols.map(c => sizes.getOrElse(c, 0L))
    val tasks = IndexedSeq.newBuilder[IndexedSeq[Int]]
    var cur   = IndexedSeq.empty[Int]
    var size  = 0L
    cols
      .indices
      .sortBy(i => -bytes(i))
      .foreach { i =>
        cur :+= i
        size += bytes(i)
        if size >= ParallelDecodeMinBytes then
          tasks += cur
          cur = IndexedSeq.empty
          size = 0L
      }
    val full = tasks.result()
    if cur.isEmpty then
      full
    else if full.isEmpty then
      IndexedSeq(cur)
    else
      full.init :+ (full.last ++ cur)

  /** Up to how many rows a string column is read to count its distinct values */
  val DistinctScanLimit: Long = 1000000

  /**
    * How long a source reuses the file version it read for its distinct-count estimates: a source
    * registered in a catalog, which outlives a query, sees a rewrite after at most this long
    */
  val VersionRecheckNanos: Long = 1000000000L

  /** Up to how many (file, column) distinct-count estimates are kept */
  val MaxDistinctEntries: Int = 4096

  private[catalog] val DistinctStat    = "distinct"
  private[catalog] val ValueCountsStat = "values"
  private[catalog] val RunLengthStat   = "runs"

  /** Up to how many row groups a string column's values are counted in, for [[ValueCounts]] */
  val ValueSampleRowGroups: Int = 4

  /** After how many sampled rows no further row group is read for [[ValueCounts]] */
  val ValueSampleRows: Long = 250000

  /** Up to how many distinct strings a [[ValueCounts]] sample holds */
  val MaxSampleValues: Int = 4096

  /**
    * Up to how many (file, column) [[ValueCounts]] samples are kept, apart from the distinct-count
    * estimates: each holds up to [[MaxSampleValues]] strings
    */
  val MaxValueCountsEntries: Int = 512

  /** A statistic and the file version (size, last-modified) it was computed from */
  private case class StatEntry(version: (Long, Long), value: Option[Any])

  // Statistics per (file path, column, statistic), shared by the sources of the same file, in
  // one LRU per statistic (access-ordered, so that the eldest entry is the least recently used)
  private def lru(max: Int) =
    new java.util.LinkedHashMap[(String, String, String), StatEntry](16, 0.75f, true):
      override def removeEldestEntry(
          e: java.util.Map.Entry[(String, String, String), StatEntry]
      ): Boolean = size() > max

  private val distinctCache    = lru(MaxDistinctEntries)
  private val valueCountsCache = lru(MaxValueCountsEntries)

  private def statCache(stat: String) =
    if stat == ValueCountsStat then
      valueCountsCache
    else
      distinctCache

  /** The cached `stat` of `column` in `path`, when it was computed from file version `version` */
  private def cachedStat(
      path: String,
      column: String,
      stat: String,
      version: (Long, Long)
  ): Option[Option[Any]] =
    val cache = statCache(stat)
    cache.synchronized {
      Option(cache.get((path, column, stat))).filter(_.version == version).map(_.value)
    }

  private def putStat(
      path: String,
      column: String,
      stat: String,
      version: (Long, Long),
      value: Option[Any]
  ): Unit =
    val cache = statCache(stat)
    cache.synchronized {
      cache.put((path, column, stat), StatEntry(version, value))
    }

  /** The file version the cached estimate of `column` in `path` was computed from (for tests) */
  def cachedDistinctVersion(path: String, column: String): Option[(Long, Long)] = distinctCache
    .synchronized {
      Option(distinctCache.get((path, column, DistinctStat))).map(_.version)
    }

  /**
    * The row groups of `f` that neither uni's row-group filter nor the decimal statistics
    * ([[DecimalStats]]) of `spec.filter` skip
    */
  def selectRowGroups(
      f: ParquetFile,
      spec: ScanSpec,
      options: ParquetReadOptions
  ): IndexedSeq[Int] =
    val groups = f.selectRowGroups(options)
    spec.filter match
      case Some(filter) if DecimalStats.hasDecimalCompare(filter, f.schema) =>
        val meta = f.rowGroups
        groups.filter(g => DecimalStats.mayMatch(filter, f.schema, meta(g)))
      case _ =>
        groups

  /** The columns, their engine types, and uni read options of a scan over `schema` */
  def readOptions(schema: Schema, spec: ScanSpec): (Seq[String], Seq[PType], ParquetReadOptions) =
    val cols  = spec.columns.getOrElse(schema.names)
    val types = cols.map(c => schema.fields.find(_.name == c).map(_.ptype).getOrElse(PType.AnyT))
    // Decimals are unscaled Long values in the engine: INT32 decimals are read as such directly
    var options = ParquetReadOptions
      .default
      .withColumns(cols*)
      .withBinaryAsString(true)
      .withInt32DecimalAsLong(true)
    spec.filter.foreach(f => options = options.withRowGroupFilter(f))
    spec.rowFilter.foreach(f => options = options.withFilter(f))
    if spec.dictionaryEntries then
      // Each row's String is its entry's one String: uni need not copy the entry's bytes per row.
      // uni reads the columns its row filter tests as before
      val strings = cols
        .zip(types)
        .collect { case (c, PType.Str) =>
          c
        }
      if strings.nonEmpty then
        options = options.withDictionaryCodesOnly(strings*)
    (cols, types, options)

  /**
    * A Parquet dataset (a folder of Parquet files, a Delta table, or an Iceberg table). What uni
    * refuses to read of an Iceberg table (ORC data files, encryption) is a NOT_IMPLEMENTED error
    * naming the table
    */
  def openDataset(path: String): ParquetDataset =
    try
      Parquet.dataset(path)
    catch
      case e: ParquetException if isIcebergPath(path) =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"The wvlet engine can't read the Iceberg table ${path}: ${e.getMessage}",
            e
          )

  private def isIcebergPath(path: String): Boolean =
    path.endsWith(".metadata.json") || IcebergWriter.isTable(path)

  def closing[A](it: Iterator[A], close: () => Unit): Iterator[A] =
    new Iterator[A]:
      private var closed   = false
      def hasNext: Boolean =
        val h = it.hasNext
        if !h && !closed then
          closed = true
          close()
        h
      def next(): A = it.next()

  def ptypeOf(f: ParquetField): PType =
    f match
      case p: ParquetField.Primitive =>
        (p.physicalType, p.logicalType) match
          case (_, Some(LogicalType.Decimal(scale, precision))) =>
            PType.decimal(precision, scale)
          case (PhysicalType.BOOLEAN, _) =>
            PType.Bool
          case (PhysicalType.INT32, Some(LogicalType.Date)) =>
            PType.Date
          case (PhysicalType.INT32, Some(LogicalType.Integer(32, false))) =>
            PType.Int64
          case (PhysicalType.INT32, Some(LogicalType.Time(_, _))) =>
            PType.AnyT
          case (PhysicalType.INT32, _) =>
            PType.Int32
          // Adjusted to UTC: an instant (TIMESTAMPTZ), as DuckDB reads it
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(true, _))) =>
            PType.TimestampTz
          case (PhysicalType.INT64, Some(LogicalType.Timestamp(_, _))) =>
            PType.Timestamp
          case (PhysicalType.INT64, Some(LogicalType.Time(_, _))) =>
            PType.AnyT
          case (PhysicalType.INT64, _) =>
            PType.Int64
          case (PhysicalType.INT96, _) =>
            PType.Timestamp
          case (PhysicalType.FLOAT, _) | (PhysicalType.DOUBLE, _) =>
            PType.Float64
          case (
                PhysicalType.BYTE_ARRAY,
                Some(LogicalType.String | LogicalType.Enum | LogicalType.Json)
              ) =>
            PType.Str
          case (PhysicalType.BYTE_ARRAY, None) =>
            PType.Str
          case (PhysicalType.FIXED_LEN_BYTE_ARRAY, Some(LogicalType.Uuid)) =>
            PType.Str
          case _ =>
            PType.AnyT
      case g: ParquetField.Group =>
        PType.AnyT

  /** The reader's null flags, shared without copying (vectors are immutable) */
  private def nullsOf(c: ColumnVector): Array[Boolean] = c.nullFlags

  /** Convert uni column vectors into engine vectors, mostly without copying the value arrays */
  def toBatch(
      b: ColumnBatch,
      cols: Seq[String],
      types: Seq[PType],
      dictionaryCodes: Boolean = false,
      lazyStrings: Boolean = false
  ): Batch =
    val vecs = cols
      .indices
      .map { k =>
        val c = b.column(cols(k))
        toVec(c, types(k), dictionaryCodes, lazyStrings)
      }
    Batch(vecs.toArray, b.numRows)

  /**
    * The engine vector of a uni column. With `dictionaryCodes`, strings decoded from a dictionary
    * page carry their dictionary codes ([[StrVec.codes]]); with `lazyStrings` too, the vector holds
    * only the codes and the entries' strings when every non-NULL row has a code
    */
  def toVec(
      c: ColumnVector,
      ptype: PType,
      dictionaryCodes: Boolean = false,
      lazyStrings: Boolean = false
  ): Vec =
    val nulls = nullsOf(c)
    (c, ptype) match
      case (x: ColumnVector.BooleanColumn, PType.Bool) =>
        BoolVec(x.values, nulls)
      case (x: ColumnVector.IntColumn, PType.Int32 | PType.Date) =>
        IntVec(ptype, x.values, nulls)
      case (x: ColumnVector.IntColumn, PType.Int64) =>
        // unsigned int32
        LongVec(PType.Int64, Vec.mapIntToLong(x.values)(Integer.toUnsignedLong), nulls)
      case (x: ColumnVector.IntColumn, d: PType.Decimal) =>
        LongVec(d, Vec.mapIntToLong(x.values)(_.toLong), nulls)
      case (x: ColumnVector.LongColumn, PType.Int64) =>
        LongVec(PType.Int64, x.values, nulls)
      case (x: ColumnVector.LongColumn, d: PType.Decimal) =>
        LongVec(d, x.values, nulls)
      case (x: ColumnVector.LongColumn, t) if t.isTimestamp =>
        val unit =
          x.field.logicalType match
            case Some(LogicalType.Timestamp(_, u)) =>
              u
            case _ =>
              TimeUnit.MICROS
        unit match
          case TimeUnit.MICROS =>
            LongVec(t, x.values, nulls)
          case TimeUnit.MILLIS =>
            LongVec(t, Vec.mapLong(x.values)(_ * 1000L), nulls)
          case TimeUnit.NANOS =>
            LongVec(t, Vec.mapLong(x.values)(Math.floorDiv(_, 1000L)), nulls)
      case (x: ColumnVector.FloatColumn, PType.Float64) =>
        DoubleVec(Vec.mapFloatToDouble(x.values), nulls)
      case (x: ColumnVector.DoubleColumn, PType.Float64) =>
        DoubleVec(x.values, nulls)
      case (x: ColumnVector.BinaryColumn, PType.Str) if dictionaryCodes && x.dictionarySize > 0 =>
        // A dictionary larger than the batch would cost more to hold than the rows' strings
        if lazyStrings && x.dictionarySize <= x.size then
          entryStrings(x, nulls)
        else
          stringsWithCodes(x, nulls)
      case (x: ColumnVector.BinaryColumn, PType.Str) =>
        val n   = x.size
        val out = new Array[String](n)
        var i   = 0
        while i < n do
          if nulls == null || !nulls(i) then
            out(i) = x.getString(i)
          i += 1
        StrVec(out, nulls)
      case (_, t) if t.isTimestamp =>
        val b = VecBuilder(t, c.size)
        var i = 0
        while i < c.size do
          c.getValue(i) match
            case null =>
              b.addNull()
            case t: java.time.Instant =>
              b.add(t.getEpochSecond * ValueOps.MicrosPerSecond + t.getNano / 1000)
            case other =>
              b.add(ValueOps.parseTimestamp(other.toString))
          i += 1
        b.result()
      case _ =>
        val b = VecBuilder(ptype, c.size)
        var i = 0
        while i < c.size do
          b.add(normalize(c.getValue(i)))
          i += 1
        b.result()
    end match

  end toVec

  /** The strings of a dictionary-encoded column with the dictionary entry of each row */
  private def stringsWithCodes(x: ColumnVector.BinaryColumn, nulls: Array[Boolean]): StrVec =
    val n     = x.size
    val out   = new Array[String](n)
    val codes = new Array[Int](n)
    var i     = 0
    while i < n do
      if nulls == null || !nulls(i) then
        out(i) = x.getString(i)
        codes(i) = x.dictionaryIndex(i)
      else
        codes(i) = -1
      i += 1
    StrVec(out, nulls, codes, x.dictionarySize)

  /**
    * The codes of a dictionary-encoded column with the strings of the entries its rows use
    * ([[StrVec.ofEntries]]), or [[stringsWithCodes]] when a non-NULL row has no entry (a PLAIN
    * value after a dictionary fallback)
    */
  private def entryStrings(x: ColumnVector.BinaryColumn, nulls: Array[Boolean]): StrVec =
    val n       = x.size
    val entries = new Array[String](x.dictionarySize)
    val codes   = new Array[Int](n)
    var i       = 0
    while i < n do
      val k = x.dictionaryIndex(i)
      if k >= 0 then
        if entries(k) == null then
          entries(k) = x.getString(i)
        codes(i) = k
      else if nulls == null || !nulls(i) then
        return stringsWithCodes(x, nulls)
      else
        codes(i) = -1
      i += 1
    StrVec.ofEntries(entries, codes, nulls, n)

  /** Nested Parquet values into engine values (Seq / Map / numbers / strings) */
  private def normalize(v: Any): Any =
    v match
      case null =>
        null
      case b: scala.math.BigDecimal =>
        b.bigDecimal
      case s: Seq[?] =>
        s.map(normalize)
      case m: Map[?, ?] =>
        m.map((k, x) => normalize(k) -> normalize(x))
      case d: java.time.LocalDate =>
        d.toString
      case t: java.time.Instant =>
        ValueOps.formatTimestamp(t.getEpochSecond * ValueOps.MicrosPerSecond + t.getNano / 1000)
      case b: Array[Byte] =>
        String(b, "UTF-8")
      case other =>
        other

end ParquetSource

/** Named tables visible to queries */
class EngineCatalog:
  private val tables = mutable.LinkedHashMap.empty[String, TableSource]

  def register(source: TableSource): Unit = tables.synchronized {
    tables(source.name.toLowerCase) = source
  }

  /** Register a Parquet file or folder as table `name`, with the column sets declared unique */
  def registerParquet(name: String, path: String, uniqueKeys: Seq[Seq[String]] = Nil): Unit =
    register(ParquetSource(name, path, declaredKeys = uniqueKeys))

  def drop(name: String): Unit = tables.synchronized {
    tables.remove(name.toLowerCase)
  }

  def find(name: String): Option[TableSource] = tables.synchronized {
    tables.get(name.toLowerCase)
  }

  def tableNames: Seq[String] = tables.synchronized(tables.values.map(_.name).toSeq)

  // Maps a (possibly qualified) table reference to its catalog key, when the owner of this
  // catalog names tables by a scheme of its own (e.g. relative to a default schema)
  private var nameKey: Option[String => String] = None

  /**
    * Name tables by `key`: a reference resolves to exactly the table `key(fullName)`, with no
    * fallback to the bare table name, so `s.t` never silently reads another schema's `t`
    */
  def withNameKey(key: String => String): EngineCatalog =
    nameKey = Some(key)
    this

  /**
    * A copy of this catalog, with the same naming, where `source` replaces the table of its name:
    * for running a query over one part of a table
    */
  def overlay(source: TableSource): EngineCatalog =
    val c = EngineCatalog()
    tables.synchronized(c.tables ++= tables)
    c.nameKey = nameKey
    c.register(source)
    c

  /**
    * The table a reference (`name`, qualified as `fullName`) points to. A registered table comes
    * first, then a built-in benchmark table (`tpch.sf1.lineitem`, see [[BenchmarkCatalogs]])
    */
  def lookup(name: String, fullName: String): Option[TableSource] =
    nameKey match
      case Some(key) =>
        find(key(fullName)).orElse(BenchmarkCatalogs.find(fullName))
      case None =>
        // A qualified reference names its table exactly; the bare name is the fallback
        find(fullName).orElse(BenchmarkCatalogs.find(fullName)).orElse(find(name))

  /**
    * information_schema views of this catalog. Tables live in the `main` schema of the `memory`
    * catalog, the names DuckDB uses for an in-memory database
    */
  def informationSchema(name: String): Option[TableSource] =
    def strTable(n: String, cols: Seq[String], rows: Seq[Seq[String]]): TableSource =
      val schema = Schema(cols.map(c => Field(c, PType.Str)).toIndexedSeq)
      val batch  = Batch(
        cols.indices.map(i => Vec.fromValues(PType.Str, rows.map(_(i)))).toArray,
        rows.size
      )
      MemTable(n, schema, Seq(batch))
    name.toLowerCase.split("\\.").takeRight(2) match
      case Array("information_schema", "schemata") =>
        Some(
          strTable(
            "schemata",
            Seq("catalog_name", "schema_name"),
            Seq(Seq(EngineCatalog.CatalogName, EngineCatalog.SchemaName))
          )
        )
      case Array("information_schema", "tables") =>
        Some(
          strTable(
            "tables",
            Seq("table_catalog", "table_schema", "table_name", "table_type"),
            tableNames.map(t =>
              Seq(EngineCatalog.CatalogName, EngineCatalog.SchemaName, t, "BASE TABLE")
            )
          )
        )
      case _ =>
        None

  def get(name: String): TableSource = find(name).getOrElse {
    throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${name}")
  }

end EngineCatalog

object EngineCatalog:
  val CatalogName = "memory"
  val SchemaName  = "main"

  /**
    * An empty compile-time catalog named `catalogName` that also resolves the built-in benchmark
    * tables (`tpch.sf1.lineitem`, see [[BenchmarkCatalogs]]), so the typer knows their columns
    */
  def compileCatalog(
      catalogName: String,
      dbType: wvlet.lang.compiler.DBType = wvlet.lang.compiler.DBType.Wvlet
  ): wvlet.lang.catalog.InMemoryCatalog =
    import wvlet.lang.catalog.Catalog
    new wvlet.lang.catalog.InMemoryCatalog(catalogName, Nil, dbType):
      override def findTable(
          tableName: Catalog.TableName,
          defaultSchema: String
      ): Option[Catalog.TableDef] =
        // Registered tables come first, as in EngineCatalog.lookup. The default lookup ignores
        // the catalog part, so another catalog's name reaches it only after the benchmarks
        if tableName.catalog.forall(_.equalsIgnoreCase(this.catalogName)) then
          super.findTable(tableName, defaultSchema)
        else
          BenchmarkCatalogs.tableDef(tableName).orElse(super.findTable(tableName, defaultSchema))

  /**
    * A compile-time catalog with the columns of `tables` in the `main` schema, for the `wvlet`
    * dialect: with it, the typer knows each column's type, so stdlib methods such as
    * `o_comment.like(...)` resolve. Pass it to `Compiler.setDefaultCatalog`. `dbType` is the SQL
    * dialect the compiler targets (the catalog decides `Context.dbType`).
    */
  def toCompileCatalog(
      tables: Seq[TableSource],
      dbType: wvlet.lang.compiler.DBType = wvlet.lang.compiler.DBType.Wvlet
  ): wvlet.lang.catalog.InMemoryCatalog =
    import wvlet.lang.catalog.Catalog
    val c = compileCatalog(CatalogName, dbType)
    c.createSchema(
      Catalog.TableSchema(Some(CatalogName), SchemaName),
      Catalog.CreateMode.CREATE_IF_NOT_EXISTS
    )
    tables.foreach { t =>
      val columns = t.schema.fields.map(f => Catalog.TableColumn(f.name, f.ptype.toDataType))
      c.createTable(
        Catalog.TableDef(Catalog.TableName(Some(CatalogName), Some(SchemaName), t.name), columns),
        Catalog.CreateMode.CREATE_IF_NOT_EXISTS
      )
    }
    c

end EngineCatalog
