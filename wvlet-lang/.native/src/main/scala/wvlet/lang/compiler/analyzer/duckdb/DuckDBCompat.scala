package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType

import java.nio.file.Files
import java.nio.file.Paths
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/**
  * Scala Native DuckDB backend. Calls libduckdb's C API directly via `DuckDBApi`. Used for non-JSON
  * file schema inference (parquet, csv, …) — JSON is handled cross-platform by `JSONAnalyzer`
  * higher up.
  *
  * Requires `libduckdb` to be installed at link time (`-lduckdb`); on macOS Homebrew puts it at
  * `/opt/homebrew/lib/libduckdb.dylib` which the system linker finds by default.
  */
trait DuckDBCompat:
  def isAvailable: Boolean = true

  def schemaOf(path: String): RelationType =
    if !Files.isRegularFile(Paths.get(path)) then
      EmptyRelationType
    else
      Zone {
        val db  = stackalloc[DuckDBApi.duckdb_database]()
        val con = stackalloc[DuckDBApi.duckdb_connection]()

        // Open in-memory database (NULL path)
        if DuckDBApi.duckdb_open(null, db) != 0 then
          throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_open failed")

        try
          if DuckDBApi.duckdb_connect(!db, con) != 0 then
            throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_connect failed")

          try
            val result = stackalloc[DuckDBApi.duckdb_result]()
            // Path is interpolated inline because DuckDB doesn't parameterize FROM clauses.
            // The Files.isRegularFile check above pins the path to something real.
            val sql = toCString(s"select * from '${path}' limit 0")

            if DuckDBApi.duckdb_query(!con, sql, result) != 0 then
              val err = fromCString(DuckDBApi.duckdb_result_error(result))
              DuckDBApi.duckdb_destroy_result(result)
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException(s"duckdb_query failed for ${path}: ${err}")

            try
              val n       = DuckDBApi.duckdb_column_count(result).toLong
              val columns = (0L until n)
                .map { i =>
                  val idx      = i.toCSize
                  val rawName  = DuckDBApi.duckdb_column_name(result, idx)
                  val rawType  = DuckDBApi.duckdb_column_type(result, idx)
                  val name     = fromCString(rawName)
                  val dataType = mapType(rawType)
                  NamedType(Name.termName(name), dataType)
                }
                .toList
              SchemaType(None, Name.typeName(RelationType.newRelationTypeName), columns)
            finally
              DuckDBApi.duckdb_destroy_result(result)
          finally
            DuckDBApi.duckdb_disconnect(con)
          end try
        finally
          DuckDBApi.duckdb_close(db)
        end try
      }

  /**
    * Map DuckDB's C-API type enum to a wvlet `DataType` via the same string-parse path the JVM
    * backend uses (`DataType.parse(jdbcLowerCaseName)`). Keeps both backends consistent and lets
    * `DataType.parse` own the resolution of compound types like `timestamp`.
    */
  private def mapType(t: DuckDBApi.duckdb_type): DataType =
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

  /**
    * `Zone`-aware string allocation. Scala Native's `c"..."` only works with literals; runtime
    * strings need to be copied into a Zone-managed buffer.
    */
  private def toCString(s: String)(using Zone): CString =
    val bytes = s.getBytes("UTF-8")
    val ptr   = alloc[Byte](bytes.length + 1)
    var i     = 0
    while i < bytes.length do
      (ptr + i).update(0, bytes(i))
      i += 1
    (ptr + bytes.length).update(0, 0.toByte)
    ptr

end DuckDBCompat
