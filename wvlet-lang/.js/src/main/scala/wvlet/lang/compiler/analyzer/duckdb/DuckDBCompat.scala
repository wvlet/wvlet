package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.QueryResultRow
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

  def canExecute: Boolean = DuckDBApi.instance.isDefined

  /**
    * Run `sql` against a fresh in-memory DuckDB and return all rows as strings via the chunk/vector
    * API — one query, no SQL rewriting. The result's `duckdb_column_type` tells us each column's
    * storage layout in the chunk; we read each row at the right offset and format the value as a
    * string.
    *
    * Type coverage:
    *   - BOOLEAN, TINYINT/UTINYINT, SMALLINT/USMALLINT, INTEGER/UINTEGER → 1/2/4-byte ints
    *   - BIGINT/UBIGINT → 8-byte int (BigInt in JS, `.toString()` for output)
    *   - FLOAT → 32-bit float; DOUBLE → 64-bit float
    *   - VARCHAR → 16-byte `duckdb_string_t` (inlined ≤ 12 bytes, or 8-byte heap pointer at offset
    *     8; read the inner pointer as `void *` via `KoffiOps.decode` so koffi keeps it as a pointer
    *     rather than auto-stringifying)
    *
    * Types not yet implemented (DATE, TIME, TIMESTAMP, DECIMAL, INTERVAL, HUGEINT, …) fall through
    * to a per-cell error. Wrap such columns with `CAST(col AS VARCHAR)` in the user SQL until those
    * readers land. Captured in `plans/2026-05-13-duckdb-execute-followups.md`.
    */
  def execute(sql: String): QueryResult =
    DuckDBApi.instance match
      case None =>
        throw new UnsupportedOperationException(
          "libduckdb is not available; set WVLET_LIBDUCKDB to its absolute path or install it via your platform package manager"
        )
      case Some(api) =>
        executeWith(api, sql)

  private def executeWith(api: DuckDBApi, sql: String): QueryResult =
    val dbBox = js.Array[js.Any](null)
    if asInt(api.duckdb_open.call(null, null, dbBox)) != 0 then
      throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_open failed")
    val db = dbBox(0)
    try
      val conBox = js.Array[js.Any](null)
      if asInt(api.duckdb_connect.call(null, db, conBox)) != 0 then
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_connect failed")
      val con = conBox(0)
      try runQuery(api, con, sql)
      finally api.duckdb_disconnect.call(null, js.Array[js.Any](con))
    finally
      api.duckdb_close.call(null, js.Array[js.Any](db))
    end try
  end executeWith

  private def runQuery(api: DuckDBApi, con: js.Any, sql: String): QueryResult =
    val result = js.Object()
    if asInt(api.duckdb_query.call(null, con, sql, result)) != 0 then
      val err = api.duckdb_result_error.call(null, result).toString
      api.duckdb_destroy_result.call(null, result)
      throw StatusCode.NOT_IMPLEMENTED.newException(s"duckdb_query failed: ${err}")
    try
      // Pull metadata once — names, declared types, and per-column chunk readers.
      val nCols    = bigIntToLong(api.duckdb_column_count.call(null, result)).toInt
      val columns  = new Array[NamedType](nCols)
      val rawTypes = new Array[Int](nCols)
      var i        = 0
      while i < nCols do
        val idx     = longToBigInt(i.toLong)
        val name    = api.duckdb_column_name.call(null, result, idx).toString
        val rawType = asInt(api.duckdb_column_type.call(null, result, idx))
        rawTypes(i) = rawType
        columns(i) = NamedType(Name.termName(name), mapType(rawType))
        i += 1

      val rows = List.newBuilder[QueryResultRow]
      var done = false
      while !done do
        val chunk = api.duckdb_fetch_chunk.call(null, result)
        if isNullPtr(chunk) then
          done = true
        else
          try readChunk(api, chunk, rawTypes, rows)
          finally api.duckdb_destroy_data_chunk.call(null, js.Array[js.Any](chunk))
      QueryResult(columns.toList, rows.result())
    finally
      api.duckdb_destroy_result.call(null, result)

  end runQuery

  private def readChunk(
      api: DuckDBApi,
      chunk: js.Any,
      rawTypes: Array[Int],
      out: scala.collection.mutable.Builder[QueryResultRow, List[QueryResultRow]]
  ): Unit =
    val nCols     = rawTypes.length
    val chunkSize = bigIntToLong(api.duckdb_data_chunk_get_size.call(null, chunk))
    // Pre-fetch each column's data pointer + validity bitmap once per chunk.
    val datas      = new Array[js.Any](nCols)
    val validities = new Array[js.Any](nCols)
    var c          = 0
    while c < nCols do
      val v = api.duckdb_data_chunk_get_vector.call(null, chunk, longToBigInt(c.toLong))
      datas(c) = api.duckdb_vector_get_data.call(null, v)
      validities(c) = api.duckdb_vector_get_validity.call(null, v)
      c += 1

    var r = 0L
    while r < chunkSize do
      val values = List.newBuilder[Option[String]]
      var col    = 0
      while col < nCols do
        values += readCell(api, datas(col), validities(col), rawTypes(col), r)
        col += 1
      out += QueryResultRow(values.result())
      r += 1

  /**
    * Read one cell from a chunk vector, dispatching on the column's declared type. Returns `None`
    * for SQL NULL (via the validity bitmap), `Some(stringValue)` otherwise.
    */
  private def readCell(
      api: DuckDBApi,
      vectorData: js.Any,
      validity: js.Any,
      rawType: Int,
      row: Long
  ): Option[String] =
    val isValid =
      isNullPtr(validity) ||
        api
          .duckdb_validity_row_is_valid
          .call(null, validity, longToBigInt(row))
          .asInstanceOf[Boolean]
    if !isValid then
      None
    else
      Some(decodeCell(vectorData, rawType, row))

  private def decodeCell(vectorData: js.Any, rawType: Int, row: Long): String =
    rawType match
      case DuckDBType.BOOLEAN =>
        KoffiOps.decode(vectorData, row.toInt, "bool").asInstanceOf[Boolean].toString
      case DuckDBType.TINYINT =>
        KoffiOps.decode(vectorData, row.toInt, "int8_t").toString
      case DuckDBType.UTINYINT =>
        KoffiOps.decode(vectorData, row.toInt, "uint8_t").toString
      case DuckDBType.SMALLINT =>
        KoffiOps.decode(vectorData, (row * 2).toInt, "int16_t").toString
      case DuckDBType.USMALLINT =>
        KoffiOps.decode(vectorData, (row * 2).toInt, "uint16_t").toString
      case DuckDBType.INTEGER =>
        KoffiOps.decode(vectorData, (row * 4).toInt, "int32_t").toString
      case DuckDBType.UINTEGER =>
        KoffiOps.decode(vectorData, (row * 4).toInt, "uint32_t").toString
      case DuckDBType.BIGINT =>
        // int64 returns as js.BigInt; .toString() drops the trailing "n" via JS semantics.
        KoffiOps.decode(vectorData, (row * 8).toInt, "int64_t").toString
      case DuckDBType.UBIGINT =>
        KoffiOps.decode(vectorData, (row * 8).toInt, "uint64_t").toString
      case DuckDBType.FLOAT =>
        KoffiOps.decode(vectorData, (row * 4).toInt, "float").toString
      case DuckDBType.DOUBLE =>
        KoffiOps.decode(vectorData, (row * 8).toInt, "double").toString
      case DuckDBType.VARCHAR =>
        readVarcharCell(vectorData, row)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"DuckDB column type code ${other} is not yet supported on the JS chunk reader — " +
              "wrap the column with CAST(... AS VARCHAR) in the source SQL as a workaround."
          )

  /**
    * Decode the 16-byte `duckdb_string_t` at row index `row`. Length is the first uint32_t; payload
    * is either 12 inlined bytes (length ≤ 12) or behind an 8-byte heap pointer at offset 8 — read
    * as `void *` so koffi keeps it as a pointer rather than auto-stringifying.
    */
  private def readVarcharCell(vectorData: js.Any, row: Long): String =
    val baseOffset = (row * 16).toInt
    val length     = asInt(KoffiOps.decode(vectorData, baseOffset, "uint32_t"))
    if length == 0 then
      ""
    else if length <= 12 then
      val bytes = KoffiOps
        .decode(vectorData, baseOffset + 4, KoffiOps.array("uint8_t", length))
        .asInstanceOf[js.Array[Int]]
      decodeUtf8(bytes)
    else
      val innerPtr = KoffiOps.decode(vectorData, baseOffset + 8, "void *")
      val bytes    = KoffiOps
        .decode(innerPtr, KoffiOps.array("uint8_t", length))
        .asInstanceOf[js.Array[Int]]
      decodeUtf8(bytes)

  /**
    * Test whether a koffi-returned pointer is null. `koffi.address(p)` is the BigInt 0 for a null
    * pointer, and the JS value itself is also usually `null`.
    */
  private def isNullPtr(p: js.Any): Boolean =
    if p == null then
      true
    else
      try
        KoffiOps.address(p) == js.BigInt(0)
      catch
        case _: Throwable =>
          false

  private def decodeUtf8(bytes: js.Array[Int]): String =
    // Build a Uint8Array via JS new Uint8Array(bytes), then run TextDecoder.
    val u8  = js.Dynamic.global.Uint8Array.from(bytes)
    val dec = js.Dynamic.newInstance(js.Dynamic.global.TextDecoder)("utf-8")
    dec.decode(u8).toString

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
