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

  /** The union of the files' ranges; None when a file has no range for the column */
  def columnRange(column: String): Option[(Long, Long)] = all(files.map(_.columnRange(column)))
    .filter(_.nonEmpty)
    .map(rs => (rs.map(_._1).min, rs.map(_._2).max))

  /** The row groups of every file, in file order */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] = all(
    files.map(_.partRanges(column))
  ).map(_.flatten)

  // Estimates of this listing, kept as a single file without a trusted version keeps them
  private val stats = scala.collection.mutable.HashMap.empty[(String, String), Option[Any]]

  /** A statistic `stat` of `column`, computed once for this listing of the folder */
  def cached[A](column: String, stat: String)(compute: => Option[A]): Option[A] = stats
    .synchronized(stats.getOrElseUpdate((column, stat), compute))
    .asInstanceOf[Option[A]]

  /**
    * Batches of `column` sampled from up to `samples` files spread over the folder, one row group
    * each (see [[ParquetSource.sampleColumn]]): files are often written in key order, as their row
    * groups are
    */
  def sampleColumn(column: String, samples: Int): Iterator[Batch] =
    val n      = files.size
    val k      = math.min(n, samples)
    val picked = (0 until k).map(i => (i.toLong * n / k).toInt).distinct
    picked.iterator.flatMap(i => files(i).sampleColumn(column, samples = 1))

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
    * The plain files of the folder or glob `path`, read as sources named `name`; None when it is an
    * object store prefix, an Iceberg or Delta table, has partition folders or no files, or when a
    * file's columns differ from `schema` (the dataset's columns) in name or type
    */
  def open(
      name: String,
      path: String,
      footerCache: Boolean,
      schema: Schema
  ): Option[ParquetFolder] =
    if path.contains("://") || ParquetSource.isIcebergPath(path) || isDelta(path) then
      None
    else
      val ds = ParquetSource.openDataset(path)
      try
        if ds.partitionColumns.nonEmpty || ds.files.isEmpty || ds.tableProperties.nonEmpty then
          None
        else
          val listing  = ds.files.map(f => (f.path, f.size))
          val expected = schema.fields.map(f => (f.name, f.ptype))
          val files    = ds.files.map(f => ParquetSource(name, f.path, footerCache))
          Option.when(
            // Each file must read as a single file, with the folder's columns
            files.forall(f =>
              f.isSingleFile && f.schema.fields.map(c => (c.name, c.ptype)) == expected
            )
          )(ParquetFolder(files, listing))
      finally ds.close()

  /** The (path, size) of each file `path` lists now (None for a non-plain folder) */
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
