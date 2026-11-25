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
import wvlet.lang.engine.vector.*
import wvlet.uni.io.FileSystem
import wvlet.uni.io.IOPath
import wvlet.uni.io.ListOptions
import wvlet.uni.cloud.parquet.DatasetFile
import wvlet.uni.cloud.iceberg.Iceberg
import wvlet.uni.cloud.iceberg.IcebergTableChange
import wvlet.uni.cloud.parquet.Parquet
import wvlet.uni.cloud.parquet.ParquetColumn
import wvlet.uni.cloud.parquet.format.Repetition
import wvlet.uni.cloud.parquet.ParquetDataset
import wvlet.uni.cloud.parquet.ParquetException
import wvlet.uni.cloud.parquet.ParquetWriteMode
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * A table of a [[Warehouse]]: an Apache Iceberg table at `path` that the engine writes. Reads go
  * through uni's Iceberg reader as for any [[ParquetSource]]
  */
class ManagedTable(name: String, path: String) extends ParquetSource(name, path):
  /** The table properties */
  lazy val properties: Map[String, String] =
    val ds = ParquetSource.openDataset(path)
    try ds.tableProperties
    finally ds.close()

/** One data file of a managed table, read with the table's columns */
private[engine] class DataFilePart(
    val name: String,
    val schema: Schema,
    dataset: ParquetDataset,
    val file: DatasetFile
) extends TableSource:
  // Iceberg has no unsigned integers, so statistics order every column as the engine does
  override def supportsFilter: Boolean = true

  def scan(spec: ScanSpec): Iterator[Batch] =
    val (cols, types, options) = ParquetSource.readOptions(schema, spec)
    val one                    = dataset.withFiles(Seq(file))
    ParquetSource.closing(
      one
        .readBatches(options)
        .map(b => ParquetSource.toBatch(b, cols, types, spec.dictionaryCodes)),
      () => one.close()
    )

/**
  * A folder of persistent tables: each table is a Hadoop Apache Iceberg table at
  * `<root>/<schema>/<table>`, and each schema is a sub-folder. The folder listing is the catalog,
  * so a table copied in by another tool is a table too.
  *
  * Table keys follow the engine session's naming: `t` for a table of the default schema, and `s.t`
  * for table t of schema s.
  */
class Warehouse(
    val root: String,
    val defaultSchema: String = "main",
    /** When set, every commit expires the table's snapshots but the newest `keepSnapshots` */
    val keepSnapshots: Option[Int] = None
) extends LogSupport:
  private def dir(path: String): IOPath = IOPath.parse(path)

  // Hidden folders (`.name`) are skipped. Table folders may start with `_` (e.g. the run-scoped
  // `__wv_flow_*` stage tables); only Iceberg table folders count as tables
  private def subFolders(path: String): Seq[String] =
    if FileSystem.isDirectory(dir(path)) then
      FileSystem
        .list(dir(path), ListOptions.default)
        .filter(p => FileSystem.isDirectory(p))
        .map(_.fileName)
        .filterNot(_.startsWith("."))
        .sorted
    else
      Nil

  // Schema folders starting with `_` are skipped
  private def schemaFolders: Seq[String] = subFolders(root).filterNot(_.startsWith("_"))

  private def join(parent: String, child: String): String = s"${parent.stripSuffix("/")}/${child}"

  // An existing child folder whose name matches ignoring case, or the name as written
  private def childName(parent: String, name: String): String = subFolders(parent)
    .find(_.equalsIgnoreCase(name))
    .getOrElse(name)

  /** The schema and table names of a table key */
  def split(key: String): (String, String) =
    key.split("\\.").toList match
      case t :: Nil =>
        (defaultSchema, t)
      case s :: t :: Nil =>
        (s, t)
      case _ =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"Table ${key} is outside this warehouse: tables are named <table> or <schema>.<table>"
          )

  private def keyOf(schema: String, table: String): String =
    if schema.equalsIgnoreCase(defaultSchema) then
      table
    else
      s"${schema}.${table}"

  private def schemaPath(name: String): String = join(root, childName(root, name))

  /** The folder of a table key, whether or not the table exists */
  def pathOf(key: String): String =
    val (s, t) = split(key)
    val sp     = schemaPath(s)
    join(sp, childName(sp, t))

  /** The schemas (sub-folders), the default schema included */
  def schemas: Seq[String] = (defaultSchema +: schemaFolders)
    .distinctBy(_.toLowerCase)
    .sortBy(_.toLowerCase)

  def hasSchema(name: String): Boolean =
    name.equalsIgnoreCase(defaultSchema) || schemaFolders.exists(_.equalsIgnoreCase(name))

  def createSchema(name: String): Unit =
    if name.isEmpty || name.contains("/") || name.contains("\\") || name.startsWith(".") then
      throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid schema name: ${name}")
    FileSystem.createDirectoryIfNotExists(dir(schemaPath(name)))

  /** Removes the folder of a schema that holds no tables */
  def dropSchema(name: String): Unit =
    val p = schemaPath(name)
    if FileSystem.isDirectory(dir(p)) then
      if tablesOf(name).nonEmpty then
        throw StatusCode.INVALID_ARGUMENT.newException(s"Schema ${name} still has tables in ${p}")
      FileSystem.deleteRecursively(dir(p))

  /** The keys of a schema's tables */
  def tablesOf(schema: String): Seq[String] =
    val sp = schemaPath(schema)
    subFolders(sp).filter(t => IcebergWriter.isTable(join(sp, t))).map(keyOf(schema, _))

  /** Every table of the warehouse, by key */
  def tables: Seq[ManagedTable] = schemas.flatMap { s =>
    tablesOf(s).map(k => ManagedTable(k, pathOf(k)))
  }

  def exists(key: String): Boolean = IcebergWriter.isTable(pathOf(key))

  def table(key: String): ManagedTable = ManagedTable(key, pathOf(key))

  // ---- Writes: each is one Iceberg commit ----

  /** A new table with no rows, or (with `replace`) a snapshot that empties an existing one */
  def create(
      key: String,
      schema: Schema,
      properties: Map[String, String],
      replace: Boolean
  ): ManagedTable =
    val (s, _) = split(key)
    if !hasSchema(s) then
      throw StatusCode.SCHEMA_NOT_FOUND.newException(s"Schema not found: ${s}")
    val mode =
      if replace then
        ParquetWriteMode.Overwrite
      else
        ParquetWriteMode.ErrorIfExists
    IcebergWriter.write(pathOf(key), schema, Nil, mode, properties = properties)
    committed(key)

  /** Replaces every row (creating the table when there is none) */
  def replace(
      key: String,
      schema: Schema,
      batches: Seq[Batch],
      properties: Map[String, String] = Map.empty
  ): ManagedTable =
    val (s, _) = split(key)
    if !hasSchema(s) then
      throw StatusCode.SCHEMA_NOT_FOUND.newException(s"Schema not found: ${s}")
    IcebergWriter.write(
      pathOf(key),
      schema,
      batches,
      ParquetWriteMode.Overwrite,
      properties = properties
    )
    committed(key)

  /** Adds rows already cast to the table schema */
  def append(key: String, schema: Schema, batches: Seq[Batch]): ManagedTable =
    IcebergWriter.write(pathOf(key), schema, batches, ParquetWriteMode.Append)
    committed(key)

  /**
    * Replaces only `files` (from [[parts]]) with `batches`, already cast to the table schema, in
    * one snapshot: a copy-on-write update or delete
    */
  def replaceFiles(
      key: String,
      schema: Schema,
      files: Seq[String],
      batches: Seq[Batch]
  ): ManagedTable =
    if files.nonEmpty then
      IcebergWriter.write(
        pathOf(key),
        schema,
        batches,
        ParquetWriteMode.Overwrite,
        replacedFiles = files
      )
      committed(key)
    else
      table(key)

  /**
    * The table's data files as parts that read with the table's schema, and a function that closes
    * them
    */
  def parts(key: String, schema: Schema): (Seq[DataFilePart], () => Unit) =
    val ds = ParquetSource.openDataset(pathOf(key))
    (ds.files.map(f => DataFilePart(key, schema, ds, f)), () => ds.close())

  /** Metadata-only changes: schema changes and table properties */
  def alter(key: String, changes: Seq[IcebergTableChange]): ManagedTable =
    if changes.nonEmpty then
      try
        Iceberg.alterTable(pathOf(key), changes*)
      catch
        case e: ParquetException =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Cannot alter table ${key}: ${e.getMessage}", e)
    table(key)

  /**
    * The table after a commit. With `keepSnapshots`, its older snapshots are expired first,
    * deleting the files only they reference. The commit has landed either way, so a failed expiry
    * is logged and retried by the next commit
    */
  private def committed(key: String): ManagedTable =
    keepSnapshots.foreach { n =>
      try
        Iceberg.expireSnapshots(pathOf(key), n)
      catch
        case NonFatal(e) =>
          // Failing here would report a landed commit as failed, and a retry would apply it twice
          warn(s"Failed to expire old snapshots of ${key}: ${e.getMessage}")
    }
    table(key)

  /**
    * Moves table `key` to `to` (both keys of this warehouse) and points its metadata at the new
    * folder. Only the current snapshot is kept, so time travel to older snapshots ends
    */
  def rename(key: String, to: String): ManagedTable =
    val (s, _) = split(to)
    if !hasSchema(s) then
      throw StatusCode.SCHEMA_NOT_FOUND.newException(s"Schema not found: ${s}")
    try
      Iceberg.renameTable(pathOf(key), pathOf(to))
    catch
      case e: ParquetException =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Cannot rename table ${key} to ${to}: ${e.getMessage}", e)
    table(to)

  /** Deletes the table's folder */
  def drop(key: String): Unit =
    try
      Iceberg.dropTable(pathOf(key))
    catch
      case e: ParquetException =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Cannot drop table ${key}: ${e.getMessage}", e)

end Warehouse

object Warehouse:
  /** The Iceberg column the engine writes for a field: optional, as every written column is */
  def columnOf(f: Field): ParquetColumn = ParquetColumn(
    f.name,
    DataFileWriter.parquetTypeOf(f),
    Repetition.OPTIONAL
  )
