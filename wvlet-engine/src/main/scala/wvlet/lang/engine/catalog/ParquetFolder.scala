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

import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetDataset
import wvlet.uni.cloud.parquet.ParquetFile
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.cloud.parquet.ParquetType
import wvlet.uni.io.FileSystem
import wvlet.uni.io.IOPath

/**
  * A folder's files as uni's dataset lists them: each file's (path, size), and for a folder of Hive
  * partitions (`key=value/` folders) the partition keys and each file's values (Long, Int,
  * LocalDate or String, as typed by [[ParquetSource.openDataset]]; null for the default partition)
  */
private[catalog] case class FolderListing(
    files: IndexedSeq[(String, Long)],
    keys: IndexedSeq[String] = IndexedSeq.empty,
    values: IndexedSeq[IndexedSeq[Any]] = IndexedSeq.empty
):
  def isPartitioned: Boolean = keys.nonEmpty

/**
  * The files of a folder (or glob) of Parquet files, each read as a single-file [[ParquetSource]]
  * for planning (`folder_file_scans`). A scan of plain files splits into the row groups of every
  * file and reads with a row filter. A scan of Hive partitions splits into its data files, each
  * read through uni's dataset reader, which adds the partition values as columns and skips the
  * files whose values the filters exclude. Planning gets the files' statistics summed or merged,
  * and a partition key's from its values. Only for folders whose files all have the folder's
  * columns (but the partition keys) with the same types, and no Iceberg or Delta metadata: uni's
  * dataset reader alone handles those (schema evolution, deletes)
  *
  * @param files
  *   the files' sources, in the dataset's file order (by path)
  * @param listing
  *   the folder's files, to tell whether it changed, and their partition values
  * @param dataset
  *   uni's dataset of this listing, which picks the files of a partitioned scan by their partition
  *   values (no file is opened for that)
  * @param schema
  *   the folder's columns: the files' columns, then the partition keys
  */
private[catalog] final class ParquetFolder(
    val files: IndexedSeq[ParquetSource],
    val listing: FolderListing,
    dataset: ParquetDataset,
    schema: Schema
):
  private def all[A](xs: IndexedSeq[Option[A]]): Option[IndexedSeq[A]] =
    if xs.forall(_.isDefined) then
      Some(xs.map(_.get))
    else
      None

  /** The sum of the files' row counts */
  lazy val estimatedRows: Option[Long] = all(files.map(_.estimatedRows)).map(_.sum)

  /**
    * The union of the files' ranges; None when a file has no range for the column. Computed once
    * per listing: join ordering asks for it thousands of times
    */
  def columnRange(column: String): Option[(Long, Long)] =
    cached(column, "range") {
      partitionLongs(column) match
        case Some(vs) =>
          val known = vs.flatten
          Option.when(known.nonEmpty)((known.min, known.max))
        case None =>
          all(files.map(_.columnRange(column)))
            .filter(_.nonEmpty)
            .map(rs => (rs.map(_._1).min, rs.map(_._2).max))
    }

  /**
    * The row groups of every file, in file order; for a partition key, one part per file of a
    * non-null partition, holding its value. Computed once per listing
    */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] =
    cached(column, "parts") {
      partitionLongs(column) match
        case Some(vs) =>
          all(files.map(_.estimatedRows)).map(rows =>
            vs.indices.flatMap(i => vs(i).map(v => ColumnPartRange(v, v, rows(i))))
          )
        case None =>
          all(files.map(_.partRanges(column))).map(_.flatten)
    }

  /** The number of distinct non-null values of a partition key; None for other columns */
  def partitionDistinct(column: String): Option[Double] =
    val k = listing.keys.indexOf(column)
    Option.when(k >= 0)(
      math.max(1.0, listing.values.map(_(k)).filter(_ != null).distinct.size.toDouble)
    )

  /**
    * Each file's value of partition key `column` as a Long (an integer, or a date in epoch days;
    * None for the default partition); None for a column that is no such key, or a string key
    */
  private def partitionLongs(column: String): Option[IndexedSeq[Option[Long]]] =
    val k = listing.keys.indexOf(column)
    if k < 0 then
      None
    else
      val vs = listing
        .values
        .map(_(k))
        .map {
          case null =>
            Some(None)
          case i: Int =>
            Some(Some(i.toLong))
          case l: Long =>
            Some(Some(l))
          case d: java.time.LocalDate =>
            Some(Some(d.toEpochDay))
          case _ =>
            None
        }
      Option.when(vs.forall(_.isDefined))(vs.map(_.get))

  // Estimates of this listing, kept as a single file without a trusted version keeps them
  private val stats = scala.collection.mutable.HashMap.empty[(String, String), Option[Any]]

  /** A statistic `stat` of `column`, computed once for this listing of the folder */
  def cached[A](column: String, stat: String)(compute: => Option[A]): Option[A] = stats
    .synchronized(stats.getOrElseUpdate((column, stat), compute))
    .asInstanceOf[Option[A]]

  /**
    * Column `column` of up to `samples` row groups spread over every file's row groups, until
    * [[ParquetSource.ValueSampleRows]] rows, as a single file samples its row groups
    * ([[ParquetSource.sampleColumn]]): data is often written in key order
    */
  def sampleColumn(column: String, samples: Int): Iterator[Batch] =
    val counts = files.map(_.rowGroupCount.toLong)
    val starts = counts.scanLeft(0L)(_ + _)
    val total  = starts.last
    val k      = math.min(total, samples.toLong).toInt
    // The picked row groups as (file, row group in the file), in file order
    val picked = (0 until k)
      .map(i => i.toLong * total / k)
      .distinct
      .map { g =>
        val file = starts.lastIndexWhere(_ <= g, files.size - 1)
        (file, (g - starts(file)).toInt)
      }
    var sampled = 0L
    picked
      .groupBy(_._1)
      .toSeq
      .sortBy(_._1)
      .iterator
      .takeWhile(_ => sampled < ParquetSource.ValueSampleRows)
      .flatMap((file, groups) => files(file).sampleRowGroups(column, groups.map(_._2)))
      .map { b =>
        sampled += b.size
        b
      }

  /**
    * One part per row group of every file that `spec` doesn't skip. Each file opens when its first
    * part runs and closes after its last, so that a folder of many files doesn't hold them all open
    */
  def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    if listing.isPartitioned then
      partitionSplits(spec)
    else
      fileSplits(spec)

  /**
    * One part per row group of every data file of a partitioned folder that the filters' partition
    * values (and the files' statistics) don't skip. Each file is read as a single file, without the
    * filters' conjuncts on partition keys, and its partition values are added as constant columns
    */
  private def partitionSplits(spec: ScanSpec): Option[ScanSplits] =
    val keys       = listing.keys
    val cols       = spec.columns.getOrElse(schema.names)
    val fileCols   = cols.filterNot(keys.contains)
    val options    = ParquetSource.readOptions(schema, spec)._3
    val chosen     = dataset.selectFiles(options).map(_.path).toSet
    val keySet     = keys.toSet
    val fileFilter = spec.filter.flatMap(ParquetFolder.withoutColumns(_, keySet))
    val fileSpec   = spec.copy(columns = Some(fileCols), filter = fileFilter, rowFilter = None)
    val selected   = files
      .indices
      .filter(i => chosen(listing.files(i)._1))
      .map(i => (i, files(i).selectedRowGroups(fileSpec)))
      .filter(_._2.nonEmpty)
    if selected.map(_._2.size).sum <= 1 then
      None
    else
      val types = cols.map(c => schema.fields.find(_.name == c).map(_.ptype).getOrElse(PType.AnyT))
      // The output columns of file i's batch b: its columns, and its partition values
      def withKeys(i: Int, b: Batch, rows: Int): Batch = Batch(
        cols
          .indices
          .map { c =>
            val k = keys.indexOf(cols(c))
            if k >= 0 then
              Vec.constant(types(c), ParquetFolder.engineValue(listing.values(i)(k)), rows)
            else
              b.column(fileCols.indexOf(cols(c)))
          }
          .toArray,
        rows
      )
      if fileCols.isEmpty then
        // Only partition keys: each row group's row count is all there is to read
        val parts = selected.flatMap((i, groups) =>
          groups.map(g => () => Iterator.single(withKeys(i, null, g._2.toInt)))
        )
        Some(ScanSplits(parts, () => (), selected.flatMap(_._2.map(_._2))))
      else
        val holders = selected.map((i, groups) => (i, FileHolder(files(i), fileSpec, groups)))
        Some(
          ScanSplits(
            holders.flatMap((i, h) =>
              h.groups
                .map(g =>
                  () =>
                    val b = h.read(g._1)
                    Iterator.single(withKeys(i, b, b.size))
                )
            ),
            () => holders.foreach(_._2.close()),
            holders.flatMap(_._2.groups.map(_._2))
          )
        )
    end if

  end partitionSplits

  /** One part per row group of every file of a plain folder that `spec` doesn't skip */
  private def fileSplits(spec: ScanSpec): Option[ScanSplits] =
    val selected = files.map(f => f -> f.selectedRowGroups(spec))
    val total    = selected.map(_._2.size).sum
    if total <= 1 then
      None
    else
      val holders = selected.filter(_._2.nonEmpty).map((f, groups) => FileHolder(f, spec, groups))
      Some(
        ScanSplits(
          holders.flatMap(h => h.groups.map(g => () => Iterator.single(h.read(g._1)))),
          () => holders.foreach(_.close()),
          holders.flatMap(_.groups.map(_._2))
        )
      )

  /** A file read by the parts of its row groups, open while some part hasn't finished */
  private final class FileHolder(
      source: ParquetSource,
      spec: ScanSpec,
      val groups: IndexedSeq[(Int, Long)]
  ):
    private var file: ParquetFile = null
    private var remaining         = groups.size
    private var closed            = false

    private def open(): ParquetFile = synchronized {
      if file == null && !closed then
        file = source.openForScan(spec)
      file
    }

    /** Row group `g` as a batch; the file closes once every part has read its row group */
    def read(g: Int): Batch =
      val f =
        open() match
          case null =>
            throw IllegalStateException(
              s"${source.path} was closed before its row groups were read"
            )
          case f =>
            f
      try source.readRowGroup(f, g, spec)
      finally synchronized {
          remaining -= 1
          if remaining == 0 then
            close()
        }

    def close(): Unit = synchronized {
      closed = true
      if file != null then
        file.close()
        file = null
    }

  end FileHolder

end ParquetFolder

private[catalog] object ParquetFolder:

  /**
    * Whether `path` may name a folder of plain files: not an object store prefix, an Iceberg table
    * or a Delta table (checked once per source, before any listing)
    */
  def mayBePlain(path: String): Boolean =
    !path.contains("://") && !ParquetSource.isIcebergPath(path) && !isDelta(path)

  /**
    * The files of `listing` read as sources named `name`; None when there are none, or when a file
    * doesn't read as a single file with the folder's `columns` ([[ParquetSource.columnTypes]]:
    * names, and Parquet physical and logical types, which decide what a row filter can test)
    */
  def open(
      name: String,
      listing: FolderListing,
      dataset: ParquetDataset,
      footerCache: Boolean,
      schema: Schema,
      columns: IndexedSeq[(String, Any)]
  ): Option[ParquetFolder] =
    // The dataset's columns are the files' columns, then the partition keys
    val k           = listing.keys.size
    val fileColumns = columns.dropRight(k)
    if listing.files.isEmpty || columns.takeRight(k).map(_._1) != listing.keys then
      None
    else
      val files = listing.files.map((p, _) => ParquetSource(name, p, footerCache))
      Option.when(files.forall(f => f.isSingleFile && f.columnTypes == fileColumns))(
        ParquetFolder(files, listing, dataset, schema)
      )

  /**
    * The files `path` lists now, with their partition values, and the dataset listing them (open,
    * for [[ParquetFolder]] to keep); None for a table with properties, which uni's dataset reader
    * reads
    */
  def listing(path: String): Option[(FolderListing, ParquetDataset)] =
    val ds = ParquetSource.openDataset(path)
    if ds.tableProperties.nonEmpty then
      ds.close()
      None
    else
      val keys = ds.partitionColumns.map(_.name)
      Some(
        FolderListing(
          ds.files.map(f => (f.path, f.size)),
          keys,
          ds.files.map(f => keys.map(k => f.partitions.getOrElse(k, null)))
        ) -> ds
      )

  /**
    * The conjuncts of `f` that refer to none of `columns` (None if no conjunct is left): a
    * row-group filter keeps only row groups that may match, so dropping a conjunct keeps more
    */
  def withoutColumns(f: ParquetFilter, columns: Set[String]): Option[ParquetFilter] =
    f match
      case ParquetFilter.And(a, b) =>
        (withoutColumns(a, columns), withoutColumns(b, columns)) match
          case (Some(x), Some(y)) =>
            Some(x && y)
          case (x, y) =>
            x.orElse(y)
      case other =>
        Option.when(!other.columnNames.exists(columns))(other)

  /** A partition value as an engine value: a date as its epoch day */
  def engineValue(v: Any): Any =
    v match
      case d: java.time.LocalDate =>
        d.toEpochDay.toInt
      case other =>
        other

  /**
    * The type of a Hive partition key read as strings, from its values (null for the default
    * partition), as DuckDB's `hive_types_autocast` types them: BIGINT when every value is an
    * integer, DATE when every value is a `yyyy-MM-dd` date; None to keep strings
    */
  def partitionType(values: Seq[Any]): Option[ParquetType.Primitive] =
    val vs = values.collect { case s: String =>
      s
    }
    if vs.isEmpty || vs.size != values.count(_ != null) then
      None
    else if vs.forall(isInteger) then
      Some(ParquetType.INT64)
    else if vs.forall(isDate) then
      Some(ParquetType.DATE)
    else
      None

  private def isInteger(v: String): Boolean =
    v.nonEmpty && v.forall(c => c.isDigit || c == '-') && v.toLongOption.isDefined

  private def isDate(v: String): Boolean =
    v.length == 10 && v(4) == '-' && v(7) == '-' && (
      try
        java.time.LocalDate.parse(v)
        true
      catch
        case _: Exception =>
          false
    )

  private def isDelta(path: String): Boolean =
    try
      FileSystem.isDirectory(IOPath.parse(s"${path.stripSuffix("/")}/_delta_log"))
    catch
      case _: Exception =>
        false

end ParquetFolder
