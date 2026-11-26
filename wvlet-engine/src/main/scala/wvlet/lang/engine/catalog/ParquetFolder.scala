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
import wvlet.uni.cloud.parquet.ParquetFile
import wvlet.uni.io.FileSystem
import wvlet.uni.io.IOPath

/**
  * The files of a folder (or glob) of plain Parquet files, each read as a single-file
  * [[ParquetSource]] (`folder_file_scans`): a scan splits into the row groups of every file, reads
  * with a row filter, and planning gets the files' statistics summed or merged. Only for folders
  * whose files all have the folder's columns with the same types, and no partition folders, Iceberg
  * or Delta metadata: uni's dataset reader handles those (partition values, schema evolution,
  * deletes)
  *
  * @param files
  *   the files' sources, in the dataset's file order (by path)
  * @param listing
  *   the (path, size) of each file, to tell whether the folder changed
  */
private[catalog] final class ParquetFolder(
    val files: IndexedSeq[ParquetSource],
    val listing: IndexedSeq[(String, Long)]
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
      all(files.map(_.columnRange(column)))
        .filter(_.nonEmpty)
        .map(rs => (rs.map(_._1).min, rs.map(_._2).max))
    }

  /** The row groups of every file, in file order, computed once per listing */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] =
    cached(column, "parts")(all(files.map(_.partRanges(column))).map(_.flatten))

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
      listing: IndexedSeq[(String, Long)],
      footerCache: Boolean,
      columns: IndexedSeq[(String, Any)]
  ): Option[ParquetFolder] =
    if listing.isEmpty then
      None
    else
      val files = listing.map((p, _) => ParquetSource(name, p, footerCache))
      Option.when(files.forall(f => f.isSingleFile && f.columnTypes == columns))(
        ParquetFolder(files, listing)
      )

  /**
    * The (path, size) of each file `path` lists now; None for a folder with partition folders or
    * table properties, which uni's dataset reader reads
    */
  def listing(path: String): Option[IndexedSeq[(String, Long)]] =
    val ds = ParquetSource.openDataset(path)
    try
      Option.when(ds.partitionColumns.isEmpty && ds.tableProperties.isEmpty)(
        ds.files.map(f => (f.path, f.size))
      )
    finally ds.close()

  private def isDelta(path: String): Boolean =
    try
      FileSystem.isDirectory(IOPath.parse(s"${path.stripSuffix("/")}/_delta_log"))
    catch
      case _: Exception =>
        false

end ParquetFolder
