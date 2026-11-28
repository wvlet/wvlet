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
import wvlet.uni.cloud.parquet.DatasetFile
import wvlet.uni.cloud.parquet.ParquetDataset
import wvlet.uni.cloud.parquet.ParquetField
import wvlet.uni.cloud.parquet.format.PhysicalType

/**
  * A folder or table of Parquet files read file by file, for planning statistics and parallel
  * scans: [[ParquetFolder]] (files sharing the folder's columns, read as single files) or
  * [[DatasetFiles]] (an Iceberg table, read through uni's dataset reader one file at a time)
  */
private[catalog] trait FolderFiles:
  /** The files listed, to tell whether the folder changed, and their partition values */
  def listing: FolderListing

  /** Releases the listing's dataset, once a newer listing replaces it */
  def close(): Unit

  /** Whether a newer listing with the same files may keep this one (false for a table format) */
  def reusable: Boolean = true

  def estimatedRows: Option[Long]
  def columnRange(column: String): Option[(Long, Long)]
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]]

  /** Column `column` of a sample of the rows, spread over the files, for value estimates */
  def sampleColumn(column: String, samples: Int): Iterator[Batch]

  /** The scan cut into parts read concurrently; None for a single part */
  def scanSplits(spec: ScanSpec): Option[ScanSplits]

  /** Whether scans return only the rows of `spec.rowFilter` */
  def rowFilters: Boolean

  /** The rows of a scan read file after file; None to read them through uni's dataset reader */
  def scanFiles(spec: ScanSpec): Option[Iterator[Batch]] = None

  /** The number of distinct non-null values of a Hive partition key; None for other columns */
  def partitionDistinct(column: String): Option[Double] =
    val k = listing.keys.indexOf(column)
    Option.when(k >= 0)(
      math.max(1.0, listing.values.map(_(k)).filter(_ != null).distinct.size.toDouble)
    )

  // Estimates of this listing, kept as a single file without a trusted version keeps them
  private val stats = scala.collection.mutable.HashMap.empty[(String, String), Option[Any]]

  /** A statistic `stat` of `column`, computed once for this listing of the folder */
  def cached[A](column: String, stat: String)(compute: => Option[A]): Option[A] = stats
    .synchronized(stats.getOrElseUpdate((column, stat), compute))
    .asInstanceOf[Option[A]]

end FolderFiles

/**
  * The data files of a local Iceberg table (`dataset_file_scans`). A scan splits into the files
  * that uni's `selectFiles` keeps (by partition values and the manifests' statistics), and each
  * part reads its file through uni's dataset reader (`withFiles`), which applies the table's
  * deletes, its field-ID column mapping and defaults, and filters rows exactly by `spec.rowFilter`.
  * Planning gets row counts, integer and date ranges, and one part range per file from the
  * manifests' statistics
  *
  * @param dataset
  *   the listing's dataset, open while this listing is current
  * @param schema
  *   the table's columns
  */
private[catalog] final class DatasetFiles(
    val listing: FolderListing,
    dataset: ParquetDataset,
    schema: Schema
) extends FolderFiles:
  private def files: IndexedSeq[DatasetFile] = dataset.files

  override def close(): Unit = dataset.close()

  // A table's deletes may change while its files don't
  override def reusable: Boolean = false

  override def rowFilters: Boolean = true

  // Partition values may be transforms of columns (days, buckets)
  override def partitionDistinct(column: String): Option[Double] = None

  /** The sum of the files' row counts, before deletes */
  lazy val estimatedRows: Option[Long] = DatasetFiles
    .all(files.map(_.stats.map(_.numRows)))
    .map(_.sum)

  /** The union of the files' ranges; None when a file has no statistics for the column */
  def columnRange(column: String): Option[(Long, Long)] =
    cached(column, "range") {
      fileRanges(column).flatMap { rs =>
        val known = rs.flatten
        Option.when(known.nonEmpty)((known.map(_.min).min, known.map(_.max).max))
      }
    }

  /** One part per file holding non-null values of the column, in file order */
  def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] =
    cached(column, "parts")(fileRanges(column).map(_.flatten))

  /**
    * Each file's range of the column with its row count (None for a file of only nulls), from the
    * manifests' statistics in the PLAIN encoding of the column's type; None when a file's is
    * unknown
    */
  private def fileRanges(column: String): Option[IndexedSeq[Option[ColumnPartRange]]] =
    val width =
      dataset.schema.field(column) match
        case Some(p: ParquetField.Primitive) if !p.isRepeated =>
          p.physicalType match
            case PhysicalType.INT32 =>
              Some(4)
            case PhysicalType.INT64 =>
              Some(8)
            case _ =>
              None
        case _ =>
          None
    width.flatMap { n =>
      DatasetFiles.all(
        files.map { f =>
          for
            st <- f.stats
            cs <- st.columns.get(column)
            r  <-
              if cs.nullCount.contains(st.numRows) then
                Some(None)
              else
                for
                  min <- cs.min
                  if min.length == n
                  max <- cs.max
                  if max.length == n
                yield Some(
                  ColumnPartRange(
                    DecimalStats.littleEndian(min, n),
                    DecimalStats.littleEndian(max, n),
                    st.numRows
                  )
                )
          yield r
        }
      )
    }

  end fileRanges

  /**
    * Column `column` of up to `samples` files spread over the table, each read until its share of
    * [[ParquetSource.ValueSampleRows]] rows, string pages as dictionary codes: data is often
    * appended in key order, so the sample takes the first rows of files across the table
    */
  def sampleColumn(column: String, samples: Int): Iterator[Batch] =
    val n      = files.size
    val k      = math.min(n, samples)
    val picked = (0 until k).map(i => (i.toLong * n / k).toInt).distinct
    val share  = math.max(1L, ParquetSource.ValueSampleRows / math.max(1, picked.size))
    val spec   = ScanSpec(Some(Seq(column)), dictionaryCodes = true)
    val (cols, types, options) = ParquetSource.readOptions(schema, spec)
    val sampled                = Vector.newBuilder[Batch]
    picked.foreach { i =>
      val ds = dataset.withFiles(Seq(files(i)))
      try
        val it   = ds.readBatches(options)
        var rows = 0L
        while rows < share && it.hasNext do
          val b = ParquetSource.toBatch(it.next(), cols, types, dictionaryCodes = true)
          rows += b.size
          sampled += b
      finally
        ds.close()
    }
    sampled.result().iterator

  /** One part per file that the partition values and statistics don't skip */
  def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    val (cols, types, options) = ParquetSource.readOptions(schema, spec)
    val selected               = dataset.selectFiles(options)
    if selected.size <= 1 then
      None
    else
      val parts = selected.map { f => () =>
        val ds = dataset.withFiles(Seq(f))
        ParquetSource.closing(
          ds.readBatches(options)
            .map(
              ParquetSource.toBatch(
                _,
                cols,
                types,
                spec.dictionaryCodes,
                spec.lazyStrings,
                spec.plainStringBytes
              )
            ),
          () => ds.close()
        )
      }
      // No part row counts: a table's deletes may remove rows the manifests count
      Some(ScanSplits(parts, () => ()))

end DatasetFiles

private[catalog] object DatasetFiles:

  /** All values, when every one is known */
  def all[A](xs: IndexedSeq[Option[A]]): Option[IndexedSeq[A]] =
    if xs.forall(_.isDefined) then
      Some(xs.map(_.get))
    else
      None

  /** The files of `listing`, an Iceberg table's, read file by file through `dataset` */
  def open(listing: FolderListing, dataset: ParquetDataset, schema: Schema): Option[DatasetFiles] =
    Option.when(listing.files.nonEmpty)(DatasetFiles(listing, dataset, schema))

end DatasetFiles
