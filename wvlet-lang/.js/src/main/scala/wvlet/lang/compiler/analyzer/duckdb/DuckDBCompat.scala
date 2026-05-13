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

  def canExecute: Boolean = DuckDBApi.instance.isDefined

  /**
    * Run `sql` against a fresh in-memory DuckDB and return all rows as strings via the chunk/vector
    * API. Two probes:
    *   - `SELECT * FROM (<sql>) LIMIT 0` for column names + original types
    *   - `SELECT COLUMNS(*)::VARCHAR FROM (<sql>)` for the data, all coerced to VARCHAR
    *
    * The VARCHAR wrap means only one chunk reader path — no per-type dispatch for int / double /
    * bool. Each duckdb_string_t cell is 16 bytes: a 4-byte length prefix, then either 12 inlined
    * bytes (length ≤ 12) or 4 prefix bytes + an 8-byte pointer to heap (length > 12). For the
    * out-of-line case we read the inner pointer as `void *` via `KoffiOps.decode`, which keeps it
    * as a koffi pointer rather than auto-stringifying.
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
      try
        val columns = querySchema(api, con, sql)
        val rows    = queryRows(api, con, sql, columns.size)
        QueryResult(columns, rows)
      finally
        api.duckdb_disconnect.call(null, js.Array[js.Any](con))
    finally
      api.duckdb_close.call(null, js.Array[js.Any](db))
    end try
  end executeWith

  private def querySchema(api: DuckDBApi, con: js.Any, sql: String): List[NamedType] =
    val result = js.Object()
    val probe  = s"select * from (${sql}) wvlet_schema_probe limit 0"
    if asInt(api.duckdb_query.call(null, con, probe, result)) != 0 then
      val err = api.duckdb_result_error.call(null, result).toString
      api.duckdb_destroy_result.call(null, result)
      throw StatusCode.NOT_IMPLEMENTED.newException(s"schema probe failed: ${err}")
    try
      val n = bigIntToLong(api.duckdb_column_count.call(null, result))
      (0L until n)
        .map { i =>
          val idx     = longToBigInt(i)
          val rawName = api.duckdb_column_name.call(null, result, idx).toString
          val rawType = asInt(api.duckdb_column_type.call(null, result, idx))
          NamedType(Name.termName(rawName), mapType(rawType))
        }
        .toList
    finally
      api.duckdb_destroy_result.call(null, result)

  private def queryRows(
      api: DuckDBApi,
      con: js.Any,
      sql: String,
      colCount: Int
  ): List[QueryResultRow] =
    val result  = js.Object()
    val dataSql = s"select columns(*)::varchar from (${sql}) wvlet_data_probe"
    if asInt(api.duckdb_query.call(null, con, dataSql, result)) != 0 then
      val err = api.duckdb_result_error.call(null, result).toString
      api.duckdb_destroy_result.call(null, result)
      throw StatusCode.NOT_IMPLEMENTED.newException(s"duckdb_query failed: ${err}")
    try
      val out  = List.newBuilder[QueryResultRow]
      var done = false
      while !done do
        val chunk = api.duckdb_fetch_chunk.call(null, result)
        if isNullPtr(chunk) then
          done = true
        else
          try readChunk(api, chunk, colCount, out)
          finally api.duckdb_destroy_data_chunk.call(null, js.Array[js.Any](chunk))
      out.result()
    finally
      api.duckdb_destroy_result.call(null, result)

  private def readChunk(
      api: DuckDBApi,
      chunk: js.Any,
      colCount: Int,
      out: scala.collection.mutable.Builder[QueryResultRow, List[QueryResultRow]]
  ): Unit =
    val chunkSize = bigIntToLong(api.duckdb_data_chunk_get_size.call(null, chunk))
    // Pre-fetch each column's data pointer + validity bitmap once.
    val vectors    = new Array[js.Any](colCount)
    val datas      = new Array[js.Any](colCount)
    val validities = new Array[js.Any](colCount)
    var c          = 0
    while c < colCount do
      val v = api.duckdb_data_chunk_get_vector.call(null, chunk, longToBigInt(c.toLong))
      vectors(c) = v
      datas(c) = api.duckdb_vector_get_data.call(null, v)
      validities(c) = api.duckdb_vector_get_validity.call(null, v)
      c += 1

    var r = 0L
    while r < chunkSize do
      val values = List.newBuilder[Option[String]]
      var col    = 0
      while col < colCount do
        values += readVarcharCell(api, datas(col), validities(col), r)
        col += 1
      out += QueryResultRow(values.result())
      r += 1

  /**
    * Read one VARCHAR cell from a chunk vector. Walks the duckdb_string_t at offset `row * 16`:
    * length is the first 4 bytes, then either 12 inlined bytes or an 8-byte heap pointer at offset 8.
    */
  private def readVarcharCell(
      api: DuckDBApi,
      vectorData: js.Any,
      validity: js.Any,
      row: Long
  ): Option[String] =
    if !isNullPtr(validity) &&
      !api
        .duckdb_validity_row_is_valid
        .call(null, validity, longToBigInt(row))
        .asInstanceOf[Boolean]
    then
      None
    else
      val baseOffset = (row * 16).toInt
      val length     = asInt(KoffiOps.decode(vectorData, baseOffset, "uint32_t"))
      if length == 0 then
        Some("")
      else if length <= 12 then
        // Inlined: 12 bytes at offset baseOffset + 4
        val bytes = KoffiOps
          .decode(vectorData, baseOffset + 4, KoffiOps.array("uint8_t", length))
          .asInstanceOf[js.Array[Int]]
        Some(decodeUtf8(bytes))
      else
        // Out-of-line: read the inner pointer at offset+8 as `void *` so koffi keeps it as
        // a pointer (a "char *" decode would auto-stringify via NUL termination).
        val innerPtr = KoffiOps.decode(vectorData, baseOffset + 8, "void *")
        val bytes    = KoffiOps
          .decode(innerPtr, KoffiOps.array("uint8_t", length))
          .asInstanceOf[js.Array[Int]]
        Some(decodeUtf8(bytes))

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
