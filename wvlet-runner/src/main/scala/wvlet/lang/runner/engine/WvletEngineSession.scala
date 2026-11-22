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
package wvlet.lang.runner.engine

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.InMemoryCatalog
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.LocalFile
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.plan.ColumnNotFound
import wvlet.lang.engine.catalog.CsvSource
import wvlet.lang.engine.catalog.JsonSource
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.*
import wvlet.lang.runner.TableRows
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * A session of the in-process columnar engine ([[WvletEngine]]) behind a connector of type `wvlet`
  * with no host (a `wvlet` connector with a host is a remote wvlet server). Queries run directly on
  * typed Wvlet plans — no SQL generation and no DuckDB.
  *
  * Tables are registered from the connector properties:
  *   - `tables`: an object mapping table names to file paths (Parquet, JSON, CSV, or an Iceberg /
  *     partitioned Parquet directory)
  *   - `dataDir`: a folder whose data files and sub-folders are registered by name (file name
  *     without extension)
  *
  * Relative paths resolve against `baseDir` (the work folder). Local files referenced in queries
  * (`from 'x.parquet'`) need no registration, and `save to <table>` keeps the result as an
  * in-memory table of this session.
  */
class WvletEngineSession(config: ConnectorConfig, baseDir: String) extends LogSupport:
  private val engineBaseDir: String = config
    .properties
    .get("baseDir")
    .map(_.toString)
    .map(resolvePath)
    .getOrElse(baseDir)

  val engine: WvletEngine = WvletEngine(baseDir = engineBaseDir)

  private val catalogName = config.catalog.getOrElse(WvletEngineSession.connectorType)
  private val schemaName  = config.schema.getOrElse("main")

  /**
    * The compile-time catalog: carries the `wvlet` dialect and the schemas of registered tables, so
    * the typer resolves their columns
    */
  val catalog: InMemoryCatalog = InMemoryCatalog(
    catalogName = catalogName,
    functions = Nil,
    catalogDBType = DBType.Wvlet
  )

  catalog.createSchema(
    Catalog.TableSchema(Some(catalogName), schemaName),
    Catalog.CreateMode.CREATE_IF_NOT_EXISTS
  )

  registerConfiguredTables()

  private def resolvePath(path: String): String =
    if SourceIO.isAbsolutePath(path) || path.contains("://") then
      path
    else
      s"${baseDir.stripSuffix("/")}/${path}"

  private def registerConfiguredTables(): Unit =
    config
      .properties
      .get("dataDir")
      .foreach { dir =>
        val folder = LocalFile(resolvePath(dir.toString))
        if !folder.isDirectory then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"dataDir of connector '${config.name}' is not a folder: ${folder.path}")
        folder
          .listFiles
          .filterNot(_.name.startsWith("."))
          .foreach { f =>
            WvletEngineSession
              .sourceOf(WvletEngineSession.tableNameOf(f.name), f.path, f.isDirectory)
              .foreach(register)
          }
      }
    config
      .properties
      .get("tables")
      .foreach {
        case m: Map[?, ?] =>
          m.foreach { case (name, path) =>
            val resolved = resolvePath(path.toString)
            val src      = WvletEngineSession
              .sourceOf(name.toString, resolved, SourceIO.isDirectory(resolved))
              .getOrElse(
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(s"Unsupported data file for table ${name}: ${path}")
              )
            register(src)
          }
        case other =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"'tables' of connector '${config.name}' must be an object of table name -> path"
            )
      }

  end registerConfiguredTables

  /** Register a table to the engine and its schema to the compile-time catalog */
  def register(source: TableSource): Unit =
    engine.catalog.register(source)
    // The schema is read lazily from file footers; a table whose schema can't be read stays
    // queryable (the engine reports the error at scan time) but untyped at compile time
    try
      val columns = source.schema.fields.map(f => Catalog.TableColumn(f.name, f.ptype.toDataType))
      if catalog.tableExists(schemaName, source.name) then
        catalog.updateColumns(schemaName, source.name, columns)
      else
        catalog.createTable(
          Catalog.TableDef(
            Catalog.TableName(Some(catalogName), Some(schemaName), source.name),
            columns
          ),
          Catalog.CreateMode.FAIL_IF_EXISTS
        )
    catch
      case NonFatal(e) =>
        warn(s"Failed to read the schema of table ${source.name}: ${e.getMessage}")

  // Report engine-internal binding errors as user errors with a status code
  private def run(r: Relation)(using ctx: Context): EngineResult =
    try
      engine.execute(r)
    catch
      case e: ColumnNotFound =>
        throw StatusCode.COLUMN_NOT_FOUND.newException(e.getMessage, e)

  /** Names of the registered tables */
  def tableNames: Seq[String] = engine.catalog.tableNames

  /** Execute a query relation, returning at most `rowLimit` rows */
  def execute(r: Relation, rowLimit: Int)(using ctx: Context): TableRows =
    val result = run(r)
    val schema = DataType.SchemaType(
      None,
      Name.NoTypeName,
      result
        .schema
        .fields
        .map(f => DataType.NamedType(Name.termName(f.name), f.ptype.toDataType))
        .toList
    )
    TableRows(schema, result.toRowMaps(rowLimit), result.numRows)

  /**
    * Run a save statement. Only `save to <table>` is supported: the result is kept as an in-memory
    * table that later queries of this session can read
    */
  def save(save: Save)(using ctx: Context): Unit =
    GenSQL.bindSaveTarget(save) match
      case s: SaveTo if s.isForTable =>
        val name   = s.targetName.split("\\.").last
        val result = run(s.child)
        register(MemTable(name, result.schema, result.batches))
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"${other
                .nodeName} is not supported by the wvlet engine yet (supported: save to <table>)"
          )

end WvletEngineSession

object WvletEngineSession:
  val connectorType: String = "wvlet"

  /**
    * True when the connector runs on the in-process engine: type `wvlet` without a host. A `wvlet`
    * connector with a host points to a remote wvlet server
    */
  def isInProcess(config: ConnectorConfig): Boolean =
    config.`type`.equalsIgnoreCase(connectorType) && config.host.isEmpty

  /**
    * The catalog name the compiler uses for a connector: its `catalog`, or `wvlet` for the
    * in-process engine, whose tables need no catalog configuration
    */
  def defaultCatalogName(config: ConnectorConfig): Option[String] = config
    .catalog
    .orElse(Option.when(isInProcess(config))(connectorType))

  /** The error for SQL statements (DDL, `execute`), which the engine can't run */
  def sqlNotSupported(sqls: Seq[String]): Exception = StatusCode
    .NOT_IMPLEMENTED
    .newException(s"SQL statements are not supported by the wvlet engine:\n${sqls.mkString(";\n")}")

  private val dataFileSuffixes = Seq(
    ".parquet",
    ".json",
    ".jsonl",
    ".ndjson",
    ".json.gz",
    ".jsonl.gz",
    ".csv",
    ".tsv",
    ".csv.gz"
  )

  /** The table name of a data file or folder: its name without the data file extension */
  def tableNameOf(fileName: String): String =
    val lower = fileName.toLowerCase
    dataFileSuffixes.find(lower.endsWith).map(s => fileName.dropRight(s.length)).getOrElse(fileName)

  /**
    * A table source for a path by its extension (same rules as file references in queries), or a
    * folder (Iceberg table or a directory of Parquet files)
    */
  def sourceOf(name: String, path: String, isDirectory: Boolean): Option[TableSource] =
    val lower = path.toLowerCase
    if isDirectory || lower.endsWith(".parquet") || lower.endsWith(".metadata.json") then
      Some(ParquetSource(name, path))
    else if lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson") ||
      lower.endsWith(".json.gz") || lower.endsWith(".jsonl.gz")
    then
      Some(JsonSource(name, path))
    else if lower.endsWith(".csv") || lower.endsWith(".tsv") || lower.endsWith(".csv.gz") then
      Some(CsvSource(name, path))
    else
      None

end WvletEngineSession
