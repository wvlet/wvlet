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
import wvlet.uni.parquet.DatasetFile
import wvlet.uni.parquet.IcebergTableChange
import wvlet.uni.parquet.Parquet
import wvlet.uni.parquet.ParquetColumn
import wvlet.uni.parquet.format.Repetition
import wvlet.uni.parquet.ParquetDataset
import wvlet.uni.parquet.ParquetException
import wvlet.uni.parquet.ParquetWriteMode

/**
  * A table of a [[Warehouse]]: an Apache Iceberg table at `path` that the engine writes. Reads go
  * through uni's Iceberg reader as for any [[ParquetSource]]
  */
class ManagedTable(name: String, val path: String) extends ParquetSource(name, path):
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
      one.readBatches(options).map(b => ParquetSource.toBatch(b, cols, types)),
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
class Warehouse(val root: String, val defaultSchema: String = "main"):
  private def dir(path: String): IOPath = IOPath.parse(path)

  private def subFolders(path: String): Seq[String] =
    if FileSystem.isDirectory(dir(path)) then
      FileSystem
        .list(dir(path), ListOptions.default)
        .filter(p => FileSystem.isDirectory(p))
        .map(_.fileName)
        .filterNot(n => n.startsWith(".") || n.startsWith("_"))
        .sorted
    else
      Nil

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
  def schemas: Seq[String] = (defaultSchema +: subFolders(root))
    .distinctBy(_.toLowerCase)
    .sortBy(_.toLowerCase)

  def hasSchema(name: String): Boolean =
    name.equalsIgnoreCase(defaultSchema) || subFolders(root).exists(_.equalsIgnoreCase(name))

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
    table(key)

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
    table(key)

  /** Adds rows already cast to the table schema */
  def append(key: String, schema: Schema, batches: Seq[Batch]): ManagedTable =
    IcebergWriter.write(pathOf(key), schema, batches, ParquetWriteMode.Append)
    table(key)

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
        Parquet.alterIcebergTable(pathOf(key), changes*)
      catch
        case e: ParquetException =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Cannot alter table ${key}: ${e.getMessage}", e)
    table(key)

  /** Deletes the table's folder */
  def drop(key: String): Unit =
    try
      Parquet.dropIcebergTable(pathOf(key))
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
