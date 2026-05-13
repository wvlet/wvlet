package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType

import scala.scalajs.js

/**
  * Scala.js DuckDB backend. Loads `libduckdb` via [[Koffi]] FFI and calls the same C API we already
  * bind from Scala Native — single-call sync, no async-to-sync bridging needed.
  *
  * Library resolution is best-effort (see [[DuckDBApi]]). If libduckdb is not available on this
  * Node runtime, [[isAvailable]] reports `false` and [[schemaOf]] throws
  * `UnsupportedOperationException`. The higher-level `DuckDBAnalyzer` keeps routing JSON files
  * through `JSONAnalyzer`, so JSON queries work even without libduckdb.
  */
trait DuckDBCompat:

  def isAvailable: Boolean = DuckDBApi.instance.isDefined

  def schemaOf(path: String): RelationType =
    if !fileExists(path) then
      EmptyRelationType
    else
      DuckDBApi.instance match
        case None =>
          throw new UnsupportedOperationException(
            s"libduckdb is not available; set WVLET_LIBDUCKDB to its absolute path or install it via your platform package manager (path: ${path})"
          )
        case Some(api) =>
          schemaOfWith(api, path)

  /**
    * Whether `execute` is implemented on this backend. JS currently isn't — DuckDB 1.5.2 neutered
    * the deprecated row-based value getters (`duckdb_value_varchar` returns null), and the
    * chunk-based replacement requires more involved FFI plumbing than fits in this iteration.
    * Tracked as a follow-up.
    */
  def canExecute: Boolean = false

  def execute(sql: String): QueryResult =
    throw new UnsupportedOperationException(
      "DuckDB.execute is not yet wired on Scala.js — DuckDB 1.5.2 disabled the deprecated " +
        "row-based value getters, and the chunk-API replacement is deferred to a follow-up. " +
        "Use the JVM or Native backend for query execution."
    )

  private def schemaOfWith(api: DuckDBApi, path: String): RelationType =
    val dbBox = js.Array[js.Any](null)
    if asInt(api.duckdb_open.call(null, null, dbBox)) != 0 then
      throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_open failed")
    val db = dbBox(0)

    try
      val conBox = js.Array[js.Any](null)
      if asInt(api.duckdb_connect.call(null, db, conBox)) != 0 then
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_connect failed")
      val con = conBox(0)

      try
        val result = js.Object()
        // Path is interpolated inline because DuckDB doesn't parameterize FROM clauses. The
        // fileExists check above pins the path to a real file, and `DuckDB.escapeSqlString`
        // doubles single quotes so paths like `O'Reilly.parquet` produce valid SQL.
        val sql = s"select * from '${DuckDB.escapeSqlString(path)}' limit 0"
        if asInt(api.duckdb_query.call(null, con, sql, result)) != 0 then
          val err = api.duckdb_result_error.call(null, result).toString
          api.duckdb_destroy_result.call(null, result)
          throw StatusCode.NOT_IMPLEMENTED.newException(s"duckdb_query failed for ${path}: ${err}")

        try
          val n       = bigIntToLong(api.duckdb_column_count.call(null, result))
          val columns = (0L until n)
            .map { i =>
              val idx      = longToBigInt(i)
              val rawName  = api.duckdb_column_name.call(null, result, idx).toString
              val rawType  = asInt(api.duckdb_column_type.call(null, result, idx))
              val dataType = mapType(rawType)
              NamedType(Name.termName(rawName), dataType)
            }
            .toList
          SchemaType(None, Name.typeName(RelationType.newRelationTypeName), columns)
        finally
          api.duckdb_destroy_result.call(null, result)
      finally
        api.duckdb_disconnect.call(null, js.Array[js.Any](con))
    finally
      api.duckdb_close.call(null, js.Array[js.Any](db))
    end try
  end schemaOfWith

  /**
    * Map DuckDB's C-API type enum to a wvlet `DataType` via the same string-parse path the JVM
    * (JDBC) and Native backends use. Keeps all three backends consistent.
    */
  private def mapType(t: Int): DataType =
    val name =
      t match
        case DuckDBType.BOOLEAN =>
          "boolean"
        case DuckDBType.TINYINT | DuckDBType.UTINYINT =>
          "tinyint"
        case DuckDBType.SMALLINT | DuckDBType.USMALLINT =>
          "smallint"
        case DuckDBType.INTEGER | DuckDBType.UINTEGER =>
          "integer"
        case DuckDBType.BIGINT | DuckDBType.UBIGINT =>
          "bigint"
        case DuckDBType.FLOAT =>
          "real"
        case DuckDBType.DOUBLE =>
          "double"
        case DuckDBType.VARCHAR =>
          "varchar"
        case DuckDBType.DATE =>
          "date"
        case DuckDBType.TIMESTAMP =>
          "timestamp"
        case _ =>
          "any"
    DataType.parse(name)

  // koffi returns `uint64_t` as a `BigInt`. Convert to Scala `Long` via the JS `Number()`
  // coercion (safe for column counts and indices well below 2^53).
  private def bigIntToLong(v: Any): Long =
    js.Dynamic.global.Number(v.asInstanceOf[js.Any]).asInstanceOf[Double].toLong

  private def longToBigInt(v: Long): js.BigInt = js.BigInt(v.toString)

  private def asInt(v: Any): Int = v.asInstanceOf[Int]

  /**
    * Best-effort sync file existence check. We avoid pulling `wvlet.uni.io.IO` into `DuckDBCompat`
    * because it has its own DuckDB-independent lifecycle; instead use Node's `fs.existsSync`
    * directly via `js.Dynamic.global`.
    */
  private def fileExists(path: String): Boolean =
    try
      val fs = js.Dynamic.global.require("fs")
      fs.existsSync(path).asInstanceOf[Boolean]
    catch
      case _: Throwable =>
        false

end DuckDBCompat
