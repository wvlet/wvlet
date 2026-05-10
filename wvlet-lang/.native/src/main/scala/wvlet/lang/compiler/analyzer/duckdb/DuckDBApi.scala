package wvlet.lang.compiler.analyzer.duckdb

import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

/**
  * Minimal Scala Native bindings to DuckDB's C API (`duckdb.h`).
  *
  * Just enough to back `DuckDBCompat.schemaOf` — open an in-memory database, run a `SELECT ...
  * LIMIT 0` against a file path, read column names and types, then dispose of everything. The full
  * C API is much larger; we add bindings as we need them.
  *
  * @link("duckdb")
  *   inserts `-lduckdb` into the Scala Native link command. Builds therefore require libduckdb to
  *   be discoverable by the linker (e.g. installed via Homebrew at
  *   `/opt/homebrew/lib/libduckdb.dylib`); see the `wvc` build settings for the include/library
  *   search path.
  */
@extern
@link("duckdb")
private[duckdb] object DuckDBApi:
  // Opaque handles. C: `typedef struct _duckdb_database *duckdb_database;` — we treat them as
  // opaque pointers (Ptr[Byte]) for binding ergonomics.
  type duckdb_database   = Ptr[Byte]
  type duckdb_connection = Ptr[Byte]

  // C: `typedef enum duckdb_state { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;`
  type duckdb_state = CInt

  // C: `typedef enum DUCKDB_TYPE { ... } duckdb_type;`
  type duckdb_type = CInt

  // C: `typedef uint64_t idx_t;`
  type idx_t = CSize

  // duckdb_result struct layout (all fields below `internal_data` are deprecated, but the
  // size still has to match for stack allocation): three idx_t, then a duckdb_column*, then a
  // char* error message, then internal_data.
  type duckdb_result = CStruct6[idx_t, idx_t, idx_t, Ptr[Byte], CString, Ptr[Byte]]

  def duckdb_open(path: CString, out_database: Ptr[duckdb_database]): duckdb_state = extern
  def duckdb_close(database: Ptr[duckdb_database]): Unit                           = extern
  def duckdb_connect(
      database: duckdb_database,
      out_connection: Ptr[duckdb_connection]
  ): duckdb_state = extern

  def duckdb_disconnect(connection: Ptr[duckdb_connection]): Unit = extern
  def duckdb_query(
      connection: duckdb_connection,
      query: CString,
      out_result: Ptr[duckdb_result]
  ): duckdb_state = extern

  def duckdb_destroy_result(result: Ptr[duckdb_result]): Unit                 = extern
  def duckdb_column_count(result: Ptr[duckdb_result]): idx_t                  = extern
  def duckdb_column_name(result: Ptr[duckdb_result], col: idx_t): CString     = extern
  def duckdb_column_type(result: Ptr[duckdb_result], col: idx_t): duckdb_type = extern
  def duckdb_result_error(result: Ptr[duckdb_result]): CString                = extern

end DuckDBApi

/**
  * Mirror of `enum DUCKDB_TYPE` from `duckdb.h`. Only the codes we map to wvlet types are named;
  * everything else falls through to `DataType.AnyType` in `DuckDBCompat`.
  */
private[duckdb] object DuckDBType:
  inline val INVALID   = 0
  inline val BOOLEAN   = 1
  inline val TINYINT   = 2
  inline val SMALLINT  = 3
  inline val INTEGER   = 4
  inline val BIGINT    = 5
  inline val UTINYINT  = 6
  inline val USMALLINT = 7
  inline val UINTEGER  = 8
  inline val UBIGINT   = 9
  inline val FLOAT     = 10
  inline val DOUBLE    = 11
  inline val TIMESTAMP = 12
  inline val DATE      = 13
  inline val TIME      = 14
  inline val INTERVAL  = 15
  inline val HUGEINT   = 16
  inline val VARCHAR   = 17
  inline val BLOB      = 18
  inline val DECIMAL   = 19
end DuckDBType
