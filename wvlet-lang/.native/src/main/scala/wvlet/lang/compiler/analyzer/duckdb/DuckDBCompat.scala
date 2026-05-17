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
  def canExecute: Boolean  = true

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
            // The Files.isRegularFile check above pins the path to a real file, and
            // `DuckDB.escapeSqlString` doubles single quotes so paths like
            // `O'Reilly.parquet` produce valid SQL.
            val sql = toCString(s"select * from '${DuckDB.escapeSqlString(path)}' limit 0")

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
    * Run `sql` against a fresh in-memory DuckDB and return all rows as strings via the chunk/vector
    * API. Same shape as the JS backend — one query, per-column type dispatch.
    *
    * `duckdb_fetch_chunk` takes the 48-byte `duckdb_result` struct BY VALUE in C, which Scala
    * Native's `@extern` ABI for `CStruct6` doesn't emit correctly (chunk returns null). We use the
    * small C wrapper `wvlet_duckdb_fetch_chunk(duckdb_result *)` from `wvlet_duckdb_helpers.c` to
    * forward by-value internally on the C side.
    */
  def execute(sql: String): QueryResult = Zone {
    val db  = stackalloc[DuckDBApi.duckdb_database]()
    val con = stackalloc[DuckDBApi.duckdb_connection]()
    if DuckDBApi.duckdb_open(null, db) != 0 then
      throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_open failed")
    try
      if DuckDBApi.duckdb_connect(!db, con) != 0 then
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb_connect failed")
      try runQuery(!con, sql)
      finally DuckDBApi.duckdb_disconnect(con)
    finally
      DuckDBApi.duckdb_close(db)
    end try
  }

  private def runQuery(con: DuckDBApi.duckdb_connection, sql: String)(using Zone): QueryResult =
    val result = stackalloc[DuckDBApi.duckdb_result]()
    if DuckDBApi.duckdb_query(con, toCString(sql), result) != 0 then
      val err = fromCString(DuckDBApi.duckdb_result_error(result))
      DuckDBApi.duckdb_destroy_result(result)
      throw StatusCode.NOT_IMPLEMENTED.newException(s"duckdb_query failed: ${err}")
    try
      val nCols    = DuckDBApi.duckdb_column_count(result).toLong.toInt
      val columns  = new Array[NamedType](nCols)
      val rawTypes = new Array[Int](nCols)
      var i        = 0
      while i < nCols do
        val idx     = i.toCSize
        val rawType = DuckDBApi.duckdb_column_type(result, idx)
        rawTypes(i) = rawType
        val name = fromCString(DuckDBApi.duckdb_column_name(result, idx))
        columns(i) = NamedType(Name.termName(name), mapType(rawType))
        i += 1

      val rows = List.newBuilder[QueryResultRow]
      var done = false
      while !done do
        val chunk = DuckDBApi.wvlet_duckdb_fetch_chunk(result)
        if chunk == null then
          done = true
        else
          try
            readChunk(chunk, rawTypes, rows)
          finally
            val box = stackalloc[DuckDBApi.duckdb_data_chunk]()
            !box = chunk
            DuckDBApi.duckdb_destroy_data_chunk(box)
      QueryResult(columns.toList, rows.result())
    finally
      DuckDBApi.duckdb_destroy_result(result)

  end runQuery

  private def readChunk(
      chunk: DuckDBApi.duckdb_data_chunk,
      rawTypes: Array[Int],
      out: scala.collection.mutable.Builder[QueryResultRow, List[QueryResultRow]]
  ): Unit =
    val nCols      = rawTypes.length
    val chunkSize  = DuckDBApi.duckdb_data_chunk_get_size(chunk).toLong
    val datas      = new Array[Ptr[Byte]](nCols)
    val validities = new Array[Ptr[ULong]](nCols)
    var c          = 0
    while c < nCols do
      val v = DuckDBApi.duckdb_data_chunk_get_vector(chunk, c.toCSize)
      datas(c) = DuckDBApi.duckdb_vector_get_data(v)
      validities(c) = DuckDBApi.duckdb_vector_get_validity(v)
      c += 1

    var r = 0L
    while r < chunkSize do
      val values = List.newBuilder[Option[String]]
      var col    = 0
      while col < nCols do
        values += readCell(datas(col), validities(col), rawTypes(col), r)
        col += 1
      out += QueryResultRow(values.result())
      r += 1

  /**
    * Read one cell from a chunk vector, dispatching on column type. Returns `None` for SQL NULL via
    * the validity bitmap.
    */
  private def readCell(
      vectorData: Ptr[Byte],
      validity: Ptr[ULong],
      rawType: Int,
      row: Long
  ): Option[String] =
    val isValid = validity == null || DuckDBApi.duckdb_validity_row_is_valid(validity, row.toCSize)
    if !isValid then
      None
    else
      Some(decodeCell(vectorData, rawType, row))

  private def decodeCell(vectorData: Ptr[Byte], rawType: Int, row: Long): String =
    rawType match
      case DuckDBType.BOOLEAN =>
        (!(vectorData + row).asInstanceOf[Ptr[Byte]] != 0).toString
      case DuckDBType.TINYINT =>
        (!(vectorData + row).asInstanceOf[Ptr[Byte]]).toString
      case DuckDBType.UTINYINT =>
        (!(vectorData + row).asInstanceOf[Ptr[UByte]]).toString
      case DuckDBType.SMALLINT =>
        (!(vectorData + row * 2).asInstanceOf[Ptr[CShort]]).toString
      case DuckDBType.USMALLINT =>
        (!(vectorData + row * 2).asInstanceOf[Ptr[UShort]]).toString
      case DuckDBType.INTEGER =>
        (!(vectorData + row * 4).asInstanceOf[Ptr[CInt]]).toString
      case DuckDBType.UINTEGER =>
        (!(vectorData + row * 4).asInstanceOf[Ptr[UInt]]).toString
      case DuckDBType.BIGINT =>
        (!(vectorData + row * 8).asInstanceOf[Ptr[Long]]).toString
      case DuckDBType.UBIGINT =>
        (!(vectorData + row * 8).asInstanceOf[Ptr[ULong]]).toString
      case DuckDBType.FLOAT =>
        (!(vectorData + row * 4).asInstanceOf[Ptr[CFloat]]).toString
      case DuckDBType.DOUBLE =>
        (!(vectorData + row * 8).asInstanceOf[Ptr[CDouble]]).toString
      case DuckDBType.VARCHAR =>
        readVarcharCell(vectorData, row)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"DuckDB column type code ${other} is not yet supported on the Native chunk reader — " +
              "wrap the column with CAST(... AS VARCHAR) in the source SQL as a workaround."
          )

  /**
    * Decode the 16-byte `duckdb_string_t` at `row`. First 4 bytes are length; payload is either 12
    * inlined bytes (length ≤ 12) or behind an 8-byte heap pointer at offset 8.
    */
  private def readVarcharCell(vectorData: Ptr[Byte], row: Long): String =
    val stringTPtr = vectorData + row * 16
    val length     = !stringTPtr.asInstanceOf[Ptr[CInt]]
    if length == 0 then
      ""
    else if length <= 12 then
      readBytesAsUtf8(stringTPtr + 4, length)
    else
      // Out-of-line: bytes 8..16 are a pointer to the heap data.
      val heapPtr = !((stringTPtr + 8).asInstanceOf[Ptr[Ptr[Byte]]])
      readBytesAsUtf8(heapPtr, length)

  private def readBytesAsUtf8(ptr: Ptr[Byte], length: Int): String =
    val bytes = new Array[Byte](length)
    var i     = 0
    while i < length do
      bytes(i) = !(ptr + i)
      i += 1
    new String(bytes, "UTF-8")

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
