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
import wvlet.lang.compiler.SourceIO
import wvlet.lang.engine.TableWriter
import wvlet.lang.engine.vector.*
import wvlet.uni.parquet.Parquet
import wvlet.uni.parquet.ParquetDatasetWriteOptions
import wvlet.uni.parquet.ParquetException
import wvlet.uni.parquet.ParquetTableFormat
import wvlet.uni.parquet.ParquetWriteMode
import wvlet.uni.parquet.ParquetWriteOptions

/**
  * Writes engine batches to a local Apache Iceberg table through uni's dataset writer: Parquet data
  * files under `data/` and a Hadoop-style commit (`metadata/vN.metadata.json`,
  * `version-hint.text`). Works on JVM, Scala.js (Node.js), and Native.
  *
  * A folder is an Iceberg table when it has a `metadata/` folder holding `.metadata.json` files.
  * The engine never deletes table files: a failed commit removes only the files it wrote.
  */
object IcebergWriter:
  private val supportedOptions = Set("format", "partition_by", "compression")

  /** Whether `path` is a folder holding an Iceberg table */
  def isTable(path: String): Boolean =
    val metadata = s"${path.stripSuffix("/")}/metadata"
    SourceIO.isDirectory(metadata) &&
    SourceIO.listFiles(metadata).exists(_.toString.endsWith(".metadata.json"))

  /**
    * `save to '<dir>' with format: 'iceberg'`: create a new Iceberg table (snapshot 1) at `path`.
    * Options: `partition_by` (comma-separated identity partition columns) and `compression`. On an
    * existing table, it commits a snapshot that replaces every row, as `save to t` does for a
    * warehouse table: the schema follows the result, and the table keeps its partition spec unless
    * `partition_by` is given or the result drops a partition column. Older snapshots and their
    * files stay. A non-empty folder that isn't an Iceberg table is an error
    */
  def create(path: String, schema: Schema, batches: Seq[Batch], opts: Map[String, Any]): Unit =
    opts
      .keys
      .filterNot(supportedOptions.contains)
      .foreach { k =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported Iceberg write option for the wvlet engine: ${k}")
      }
    val exists = isTable(path)
    if !exists && SourceIO.existsFile(path) &&
      (!SourceIO.isDirectory(path) || SourceIO.listFiles(path).nonEmpty)
    then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"Cannot create an Iceberg table at ${path}: the path is not an empty folder")
    val partitionBy = opts
      .get("partition_by")
      .toSeq
      .flatMap(_.toString.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
    partitionBy.foreach { c =>
      if !schema.names.exists(_ == c) then
        throw StatusCode
          .COLUMN_NOT_FOUND
          .newException(s"Partition column ${c} is not found in the result to save")
    }
    write(
      path,
      schema,
      batches,
      if exists then
        ParquetWriteMode.Overwrite
      else
        ParquetWriteMode.ErrorIfExists
      ,
      partitionBy,
      DataFileWriter.writeOptionsOf(opts),
      // A replaced warehouse table's declared defaults and constraints no longer apply, as with
      // `save to t`
      properties =
        if exists then
          Map(TableWriter.SpecProperty -> "")
        else
          Map.empty
    )

  end create

  /**
    * `append to '<dir>'`: commit `batches` as a new snapshot of the Iceberg table at `path`.
    * `schema` must be the table's schema (names and types); uni keeps the table's partition spec
    */
  def append(path: String, schema: Schema, batches: Seq[Batch]): Unit = write(
    path,
    schema,
    batches,
    ParquetWriteMode.Append,
    Nil,
    DataFileWriter.writeOptionsOf(Map.empty)
  )

  /**
    * Commit `batches` to the Iceberg table at `path` with uni's `mode`. With `replacedFiles`
    * (Overwrite only), the commit removes just those data files, as [[ParquetSource.dataFiles]]
    * lists them; `properties` are table properties the commit sets
    */
  private[engine] def write(
      path: String,
      schema: Schema,
      batches: Seq[Batch],
      mode: ParquetWriteMode,
      partitionBy: Seq[String] = Nil,
      writeOptions: wvlet.uni.parquet.ParquetWriteOptions = ParquetWriteOptions.default,
      replacedFiles: Seq[String] = Nil,
      properties: Map[String, String] = Map.empty
  ): Unit =
    val pschema = DataFileWriter.parquetSchemaOf(schema)
    var options = ParquetDatasetWriteOptions
      .default
      .withTableFormat(ParquetTableFormat.Iceberg)
      .withMode(mode)
      .withPartitionBy(partitionBy*)
      .withWriteOptions(writeOptions)
      .withTableProperties(properties)
    if replacedFiles.nonEmpty then
      options = options.withReplacedFiles(replacedFiles)
    try
      val writer = Parquet.newDatasetBatchWriter(path, pschema, options)
      try
        batches.foreach { b =>
          if b.size > 0 then
            writer.write(DataFileWriter.columnBatchOf(pschema, schema, b))
        }
        writer.close()
      catch
        case e: Throwable =>
          try
            writer.abort()
          catch
            case s: Throwable =>
              e.addSuppressed(s)
          throw e
    catch
      case e: ParquetException =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Cannot write the Iceberg table ${path}: ${e.getMessage}", e)

  end write

end IcebergWriter
