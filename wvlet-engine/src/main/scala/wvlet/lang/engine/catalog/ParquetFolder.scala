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
  * and a partition key's from its values. Only for folders whose files have the folder's columns
  * (but the partition keys) with the same types, and no Iceberg or Delta metadata ([[DatasetFiles]]
  * reads Iceberg tables). With `dataset_file_scans`, files may lack some of the columns (schema
  * evolution, as uni's dataset reader matches columns by name): a scan splits into their row groups
  * as a partitioned one does, reading the missing columns as NULLs, without a row filter
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
  * @param missing
  *   each file's folder columns that the file lacks (none in a folder without schema evolution)
  * @param evolved
  *   whether a file lacks columns or holds them in another order, so that batches are put together
  *   by column name
  */
private[catalog] final class ParquetFolder(
    val files: IndexedSeq[ParquetSource],
    val listing: FolderListing,
    dataset: ParquetDataset,
    schema: Schema,
    missing: IndexedSeq[Set[String]],
    evolved: Boolean
) extends FolderFiles:

  // The files holding column `column`
  private def holding(column: String): IndexedSeq[ParquetSource] = files
    .indices
    .filterNot(i => missing(i)(column))
    .map(files)

  private def all[A](xs: IndexedSeq[Option[A]]): Option[IndexedSeq[A]] = DatasetFiles.all(xs)

  def close(): Unit = dataset.close()

  /** A partitioned or schema-evolved folder's scans don't take a row filter yet */
  def rowFilters: Boolean = !listing.isPartitioned && !evolved

  /** A plain folder's files, each read as a single file */
  override def scanFiles(spec: ScanSpec): Option[Iterator[Batch]] =
    Option.when(!listing.isPartitioned && !evolved)(files.iterator.flatMap(_.scan(spec)))

  /** The sum of the files' row counts */
  lazy val estimatedRows: Option[Long] = all(files.map(_.estimatedRows)).map(_.sum)

  /**
    * The union of the files' ranges; None when a file has no range for the column. Computed once
    * per listing: join ordering asks for it thousands of times
    */
  def columnRange(column: String): Option[(Long, Long)] =
    cached(column, "range") {
      ParquetFolder.partitionLongs(listing, column) match
        case Some(vs) =>
          val known = vs.flatten
          Option.when(known.nonEmpty)((known.min, known.max))
        case None =>
          all(holding(column).map(_.columnRange(column)))
            .filter(_.nonEmpty)
            .map(rs => (rs.map(_._1).min, rs.map(_._2).max))
    }

  /**
    * The row groups of every file, in file order; for a partition key, one part per file of a
    * non-null partition, holding its value. Computed once per listing
    */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] =
    cached(column, "parts") {
      ParquetFolder.partitionLongs(listing, column) match
        case Some(vs) =>
          all(files.map(_.estimatedRows)).map(rows =>
            vs.indices.flatMap(i => vs(i).map(v => ColumnPartRange(v, v, rows(i))))
          )
        case None =>
          all(holding(column).map(_.partRanges(column))).map(_.flatten)
    }

  /**
    * Column `column` of up to `samples` row groups spread over every file's row groups, until
    * [[ParquetSource.ValueSampleRows]] rows, as a single file samples its row groups
    * ([[ParquetSource.sampleColumn]]): data is often written in key order
    */
  def sampleColumn(column: String, samples: Int): Iterator[Batch] =
    // The files don't hold the partition keys
    require(!listing.keys.contains(column), s"${column} is a partition key")
    // Only the files holding the column
    val counts = files
      .indices
      .map(i =>
        if missing(i)(column) then
          0L
        else
          files(i).rowGroupCount.toLong
      )
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

  end sampleColumn

  /**
    * One part per row group of every file that `spec` doesn't skip. Each file opens when its first
    * part runs and closes after its last, so that a folder of many files doesn't hold them all open
    */
  def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    if listing.isPartitioned || evolved then
      partitionSplits(spec)
    else
      fileSplits(spec)

  /**
    * One part per row group of every data file of a partitioned (or schema-evolved) folder that the
    * filters' partition values (and the files' statistics) don't skip. Each file is read as a
    * single file of the columns it holds, without the filters' conjuncts on partition keys or on
    * columns it lacks; its partition values are added as constant columns, and the columns it lacks
    * as NULLs
    */
  private def partitionSplits(spec: ScanSpec): Option[ScanSplits] =
    val keys    = listing.keys
    val cols    = spec.columns.getOrElse(schema.names)
    val options = ParquetSource.readOptions(schema, spec)._3
    val chosen  =
      if listing.isPartitioned then
        dataset.selectFiles(options).map(_.path).toSet
      else
        listing.files.map(_._1).toSet
    val keySet = keys.toSet
    // Each file's columns read, and its spec
    def fileSpec(i: Int): ScanSpec =
      val skipped = keySet ++ missing(i)
      spec.copy(
        columns = Some(cols.filterNot(skipped)),
        filter = spec.filter.flatMap(ParquetFolder.withoutColumns(_, skipped)),
        rowFilter = None
      )
    val selected = files
      .indices
      .filter(i => chosen(listing.files(i)._1))
      .map { i =>
        val fs = fileSpec(i)
        (i, fs, files(i).selectedRowGroups(fs))
      }
      .filter(_._3.nonEmpty)
    if selected.map(_._3.size).sum <= 1 then
      None
    else
      val types = cols.map(c => schema.fields.find(_.name == c).map(_.ptype).getOrElse(PType.AnyT))
      val keyOf = cols.map(keys.indexOf)
      // The output columns of file i's batch b (of its columns `read`): its columns, its partition
      // values, and NULLs for the columns it lacks
      def withKeys(i: Int, read: Seq[String], b: Batch, rows: Int): Batch = Batch(
        cols
          .indices
          .map { c =>
            if keyOf(c) >= 0 then
              Vec.constant(types(c), ParquetFolder.engineValue(listing.values(i)(keyOf(c))), rows)
            else
              read.indexOf(cols(c)) match
                case -1 =>
                  Vec.constant(types(c), null, rows)
                case k =>
                  b.column(k)
          }
          .toArray,
        rows
      )
      val parts = IndexedSeq.newBuilder[() => Iterator[Batch]]
      val held  = IndexedSeq.newBuilder[FileHolder]
      selected.foreach { (i, fs, groups) =>
        val read = fs.columns.get
        if read.isEmpty then
          // Only partition keys and missing columns: each row group's row count is all there is
          groups.foreach(g => parts += (() => Iterator.single(withKeys(i, read, null, g._2.toInt))))
        else
          val h = FileHolder(files(i), fs, groups)
          held += h
          h.groups
            .foreach(g =>
              parts += (
                () =>
                  val b = h.read(g._1)
                  Iterator.single(withKeys(i, read, b, b.size))
              )
            )
      }
      val holders = held.result()
      Some(
        ScanSplits(
          parts.result(),
          () => holders.foreach(_.close()),
          selected.flatMap(_._3.map(_._2))
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
    * Each file's value of partition key `column` as a Long (an integer, or a date in epoch days;
    * None for the default partition); None for a column that is no such key, or a string key
    */
  def partitionLongs(listing: FolderListing, column: String): Option[IndexedSeq[Option[Long]]] =
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

  /**
    * Whether `path` may name a folder of plain files: not an object store prefix, an Iceberg table
    * or a Delta table (checked once per source, before any listing)
    */
  def mayBePlain(path: String): Boolean =
    !path.contains("://") && !ParquetSource.isIcebergPath(path) && !isDelta(path)

  /**
    * Whether `path` may name a local folder or Iceberg table to read file by file: not an object
    * store prefix or a Delta table
    */
  def mayReadAsFiles(path: String): Boolean = !path.contains("://") && !isDelta(path)

  /**
    * The files of `listing` read as sources named `name`; None when there are none, or when a file
    * doesn't read as a single file with the folder's `columns` ([[ParquetSource.columnTypes]]:
    * names, and Parquet physical and logical types, which decide what a row filter can test). With
    * `evolved`, a file may hold only some of the columns
    */
  def open(
      name: String,
      listing: FolderListing,
      dataset: ParquetDataset,
      footerCache: Boolean,
      schema: Schema,
      columns: IndexedSeq[(String, Any)],
      evolved: Boolean = false
  ): Option[ParquetFolder] =
    // The dataset's columns are the files' columns, then the partition keys
    val k           = listing.keys.size
    val fileColumns = columns.dropRight(k)
    // The keys' values must still have the source's key types (fixed when its schema was read)
    val keyTypes = schema.fields.takeRight(k).map(_.ptype)
    if listing.files.isEmpty || columns.takeRight(k).map(_._1) != listing.keys ||
      !listing.values.forall(_.zip(keyTypes).forall(hasType))
    then
      None
    else
      val files = listing.files.map((p, _) => ParquetSource(name, p, footerCache))
      if files.forall(f => f.isSingleFile && f.columnTypes == fileColumns) then
        Some(ParquetFolder(files, listing, dataset, schema, files.map(_ => Set.empty), false))
      else if evolved &&
        files.forall(f => f.isSingleFile && f.columnTypes.forall(fileColumns.contains))
      then
        // Each file's columns are some of the folder's, with the same types, in any order
        val names = fileColumns.map(_._1)
        Some(
          ParquetFolder(
            files,
            listing,
            dataset,
            schema,
            files.map(f => names.toSet -- f.columnTypes.map(_._1)),
            evolved = true
          )
        )
      else
        None

  end open

  /**
    * The files `path` lists now, with their partition values, and the dataset listing them (open,
    * for [[ParquetFolder]] or [[DatasetFiles]] to keep)
    */
  def listing(path: String): (FolderListing, ParquetDataset) =
    val ds   = ParquetSource.openDataset(path)
    val keys = ds.partitionColumns.map(_.name)
    FolderListing(
      ds.files.map(f => (f.path, f.size)),
      keys,
      ds.files.map(f => keys.map(k => f.partitions.getOrElse(k, null)))
    ) -> ds

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

  /** Whether partition value `v` (null for the default partition) is one of type `t` */
  private def hasType(v: Any, t: PType): Boolean =
    (v, t) match
      case (null, _) =>
        true
      case (_: Long, PType.Int64) | (_: Int, PType.Int32) =>
        true
      case (_: java.time.LocalDate, PType.Date) | (_: String, PType.Str) =>
        true
      case _ =>
        false

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
