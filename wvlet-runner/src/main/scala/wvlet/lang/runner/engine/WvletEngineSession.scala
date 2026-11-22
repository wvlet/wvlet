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
import scala.collection.mutable
import wvlet.lang.engine.vector.ValueOps
import wvlet.lang.engine.vector.PType
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.compiler.analyzer.TableBindings
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.TableWriter
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.plan.ColumnNotFound
import wvlet.lang.engine.catalog.CsvSource
import wvlet.lang.engine.catalog.JsonSource
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
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
  val engine: WvletEngine = WvletEngine(baseDir = baseDir)

  private val catalogName = config.catalog.getOrElse(WvletEngineSession.connectorType)
  private val schemaName  = config.schema.getOrElse("main")

  // Table references resolve with the same naming as writes (see tableKey)
  engine.catalog.withNameKey(tableKey)

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

  /**
    * Register a table to the engine, and its schema to the compile-time catalog when reading it is
    * cheap: Parquet footers and in-memory tables. JSON / CSV schemas need a full parse, so those
    * tables stay untyped at compile time and are read only when a query scans them
    */
  def register(source: TableSource): Unit =
    engine.catalog.register(source)
    source match
      case _: ParquetSource | _: MemTable =>
        registerSchema(source)
      case _ =>

  // A table whose schema can't be read stays queryable (the engine reports the error at scan
  // time) but untyped at compile time
  private def registerSchema(source: TableSource): Unit =
    try
      val columns = source.schema.fields.map(f => Catalog.TableColumn(f.name, f.ptype.toDataType))
      // Tables keyed `s.t` (see tableKey) belong to schema s
      val (schema, table) =
        source.name.lastIndexOf('.') match
          case -1 =>
            (schemaName, source.name)
          case i =>
            (source.name.substring(0, i), source.name.substring(i + 1))
      catalog.createSchema(
        Catalog.TableSchema(Some(catalogName), schema),
        Catalog.CreateMode.CREATE_IF_NOT_EXISTS
      )
      if catalog.tableExists(schema, table) then
        catalog.updateColumns(schema, table, columns)
      else
        catalog.createTable(
          Catalog.TableDef(Catalog.TableName(Some(catalogName), Some(schema), table), columns),
          Catalog.CreateMode.FAIL_IF_EXISTS
        )
    catch
      case NonFatal(e) =>
        warn(s"Failed to read the schema of table ${source.name}: ${e.getMessage}")

  private val writer = TableWriter(engine)

  // Report engine-internal binding errors as user errors with a status code
  private def guarded[A](body: => A): A =
    try
      body
    catch
      case e: ColumnNotFound =>
        throw StatusCode.COLUMN_NOT_FOUND.newException(e.getMessage, e)

  private def run(r: Relation)(using ctx: Context): EngineResult = guarded(engine.execute(r))

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
    * Run a write statement: `save to <table>`, `append to <table>` (plain and keyed), and `delete`
    * on the session's in-memory tables, which later queries read, and `save to` / `append to` a
    * data file path
    */
  def save(save: Save)(using ctx: Context): Unit =
    def tableName(s: Save): String = tableKey(s.targetName)
    guarded {
      GenSQL.bindSaveTarget(save) match
        case s: SaveTo if s.isForFile =>
          writer.saveToFile(ctx.dataFilePath(s.targetName), s.child, saveOptionsOf(s))
        case a: AppendTo if a.isForFile && a.columns.isEmpty && a.keyColumns.isEmpty =>
          writer.appendToFile(ctx.dataFilePath(a.targetName), a.child)
        case s: SaveTo if s.isForTable =>
          registerSchema(writer.saveTo(tableName(s), s.child, ifNotExists = s.ifNotExists))
        case a: AppendTo if a.isForTable =>
          // Appending to a declared table that doesn't exist yet creates it from the declaration
          if engine.catalog.find(tableName(a)).isEmpty then
            TableBindings
              .declarationFor(a.targetName)
              .foreach(d => writer.createTable(tableName(a), tableSpecOf(a.targetName, d.columns)))
          registerSchema(
            writer.appendTo(
              tableName(a),
              a.child,
              columns = a.columns.map(_.leafName),
              keys = a.keyColumns.map(_.leafName)
            )
          )
        case d: Delete if d.isForTable =>
          registerSchema(writer.delete(tableName(d), d.child))
        case other =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(
              s"${other.nodeName} is not supported by the wvlet engine yet " +
                "(supported: save to, append to, and delete on tables, and save to / append to a file)"
            )
    }

  end save

  /**
    * The engine catalog key of a table name: the name relative to this session's catalog and
    * default schema, so `t`, `main.t`, and `wvlet.main.t` are the same table, and `s.t` names table
    * t of schema s
    */
  def tableKey(name: String): String =
    val parts    = name.split("\\.").toList
    val relative =
      parts match
        case c :: s :: rest if rest.nonEmpty && c.equalsIgnoreCase(catalogName) =>
          s :: rest
        case other =>
          other
    relative match
      case s :: t :: Nil if s.equalsIgnoreCase(schemaName) =>
        t
      case other =>
        other.mkString(".")

  // A schema name relative to this session's catalog: `wvlet.s` and `s` are the same schema
  private def schemaKey(name: String): String =
    name.split("\\.").toList match
      case c :: s :: Nil if c.equalsIgnoreCase(catalogName) =>
        s
      case other =>
        other.mkString(".")

  /**
    * Run a catalog statement: `create table` (from declared columns, with defaults and NOT NULL /
    * primary key / unique constraints), `drop table`, `truncate`, and `create` / `drop schema`
    */
  def ddl(statement: TopLevelStatement)(using ctx: Context): Unit = guarded {
    statement match
      case c: CreateTable =>
        c.tableElems
          .collectFirst {
            case e if !e.isInstanceOf[ColumnDef] =>
              e
          }
          .foreach { e =>
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(
                s"${e.nodeName} in create table is not supported by the wvlet engine yet",
                c.sourceLocation
              )
          }
        if c.properties.nonEmpty then
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(
              "Table properties are not supported by the wvlet engine",
              c.sourceLocation
            )
        val key  = tableKey(c.table.fullName)
        val spec = tableSpecOf(
          c.table.fullName,
          c.tableElems
            .collect { case d: ColumnDef =>
              d
            }
        )
        writer
          .createTable(key, spec, ifNotExists = c.ifNotExists, replace = c.replace)
          .foreach(registerSchema)
      case d: DropTable =>
        writer.dropTable(tableKey(d.table.fullName), ifExists = d.ifExists)
      case t: Truncate if t.isForTable =>
        registerSchema(writer.truncate(tableKey(t.targetName)))
      case c: CreateSchema if c.location.isEmpty && c.options.isEmpty && c.properties.isEmpty =>
        val name = schemaKey(c.schema.fullName)
        if schemas.contains(name.toLowerCase) && !c.ifNotExists then
          throw StatusCode.SCHEMA_ALREADY_EXISTS.newException(s"Schema ${name} already exists")
        schemas += name.toLowerCase
      case d: DropSchema if schemaKey(d.schema.fullName).equalsIgnoreCase(schemaName) =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"The default schema ${schemaName} can't be dropped on the wvlet engine")
      case d: DropSchema =>
        val name   = schemaKey(d.schema.fullName)
        val prefix = s"${name.toLowerCase}."
        val tables = engine.catalog.tableNames.filter(_.toLowerCase.startsWith(prefix))
        if !schemas.contains(name.toLowerCase) && tables.isEmpty then
          if !d.ifExists then
            throw StatusCode.SCHEMA_NOT_FOUND.newException(s"Schema not found: ${name}")
        else if tables.nonEmpty && !d.cascade then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Schema ${name} still has tables (${tables.mkString(
                  ", "
                )}); drop it with cascade: true"
            )
        else
          tables.foreach(t => writer.dropTable(t))
          schemas -= name.toLowerCase
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"${other
                .nodeName} is not supported by the wvlet engine yet (supported: create / drop " +
              "table, truncate, and create / drop schema)"
          )
  }

  // Schemas made by `create schema`; tables of a schema are keyed `<schema>.<table>`
  private val schemas = mutable.Set.empty[String]

  // The engine table spec of declared columns: types, literal defaults, and constraints
  private def tableSpecOf(name: String, columns: List[ColumnDef])(using
      Context
  ): TableWriter.TableSpec =
    if columns.isEmpty then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"No columns are declared for table ${name}: declare it with `table ${name} = { ... }`"
        )
    val specs = columns.map { c =>
      val ptype   = PType.fromDataType(c.columnType)
      val default =
        c.defaultValue match
          case None =>
            null
          case Some(e) =>
            val v = literalValueOf(ExpressionEvaluator.eval(e)).getOrElse(
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException(
                  s"The default of column ${c
                      .columnName
                      .fullName} must be a literal on the wvlet engine",
                  e.sourceLocation
                )
            )
            ValueOps.cast(v, ptypeOfLiteral(v), ptype)
      TableWriter.ColumnSpec(
        c.columnName.leafName,
        ptype,
        default,
        notNull = c.notNull || c.primaryKey
      )
    }
    // Column-level `primary key` marks one column, as in SQL: a table has at most one
    val keys =
      columns.filter(_.primaryKey).map(_.columnName.leafName) match
        case Nil =>
          Nil
        case pk :: Nil =>
          List(List(pk))
        case pks =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Table ${name} declares more than one primary key column: ${pks.mkString(", ")}"
            )
    val uniques = columns.filter(_.unique).map(c => List(c.columnName.leafName))
    TableWriter.TableSpec(specs, keys ++ uniques)

  end tableSpecOf

  private def ptypeOfLiteral(v: Any): PType =
    v match
      case _: Boolean =>
        PType.Bool
      case _: Long =>
        PType.Int64
      case _: Double =>
        PType.Float64
      case d: java.math.BigDecimal =>
        PType.decimal(math.max(d.precision, d.scale), d.scale)
      case null =>
        PType.Null
      case _ =>
        PType.Str

  // A literal expression as a Scala value
  private def literalValueOf(e: Expression): Option[Any] =
    e match
      case l: StringLiteral =>
        Some(l.unquotedValue)
      case l: LongLiteral =>
        Some(l.value)
      case l: DoubleLiteral =>
        Some(l.value)
      case l: DecimalLiteral =>
        Some(java.math.BigDecimal(l.stringValue))
      case _: TrueLiteral =>
        Some(true)
      case _: FalseLiteral =>
        Some(false)
      case _: NullLiteral =>
        Some(null)
      case _ =>
        None

  // `save to ... with k: v, ...` option values: literals as Scala values
  private def saveOptionsOf(s: SaveTo)(using Context): Map[String, Any] =
    s.saveOptions
      .map { o =>
        val value: Any = literalValueOf(o.value).getOrElse(
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Option ${o.key.fullName} must be a literal value", o.sourceLocation)
        )
        o.key.fullName -> value
      }
      .toMap

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
    ".ndjson.gz",
    ".csv",
    ".tsv",
    ".csv.gz",
    ".tsv.gz"
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
    val lower                                   = path.toLowerCase
    def endsWithAny(suffixes: String*): Boolean = suffixes.exists(lower.endsWith)
    if isDirectory || endsWithAny(".parquet", ".metadata.json") || lower.contains("*.parquet") then
      Some(ParquetSource(name, path))
    else if endsWithAny(".json", ".jsonl", ".ndjson", ".json.gz", ".jsonl.gz", ".ndjson.gz") then
      Some(JsonSource(name, path))
    else if endsWithAny(".csv", ".tsv", ".csv.gz", ".tsv.gz") then
      Some(CsvSource(name, path))
    else
      None

end WvletEngineSession
