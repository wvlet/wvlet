package wvlet.lang.compiler.analyzer.duckdb

import scala.scalanative.unsafe.*
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.unsigned.*

/**
  * Minimal Scala Native bindings to DuckDB's C API (`duckdb.h`), loaded at runtime.
  *
  * wvlet's native binaries do not link libduckdb, so commands that never touch DuckDB start on
  * machines without it. The C loader in `wvlet_duckdb_loader.c` opens libduckdb with `dlopen`
  * (`LoadLibrary` on Windows) and each binding below calls its `wvlet_duckdb_*` forwarding wrapper.
  * Call [[DuckDBLoader.isLoaded]] before any other binding.
  */
@extern
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

  /** Open libduckdb at `path` and resolve its functions. Returns 0 on success. */
  def wvlet_duckdb_load(path: CString): CInt = extern

  /** Why the last failed [[wvlet_duckdb_load]] call failed. */
  def wvlet_duckdb_load_error(): CString = extern

  @name("wvlet_duckdb_open")
  def duckdb_open(path: CString, out_database: Ptr[duckdb_database]): duckdb_state = extern

  @name("wvlet_duckdb_close")
  def duckdb_close(database: Ptr[duckdb_database]): Unit = extern

  @name("wvlet_duckdb_connect")
  def duckdb_connect(
      database: duckdb_database,
      out_connection: Ptr[duckdb_connection]
  ): duckdb_state = extern

  @name("wvlet_duckdb_disconnect")
  def duckdb_disconnect(connection: Ptr[duckdb_connection]): Unit = extern

  @name("wvlet_duckdb_query")
  def duckdb_query(
      connection: duckdb_connection,
      query: CString,
      out_result: Ptr[duckdb_result]
  ): duckdb_state = extern

  @name("wvlet_duckdb_destroy_result")
  def duckdb_destroy_result(result: Ptr[duckdb_result]): Unit = extern

  @name("wvlet_duckdb_column_count")
  def duckdb_column_count(result: Ptr[duckdb_result]): idx_t = extern

  @name("wvlet_duckdb_column_name")
  def duckdb_column_name(result: Ptr[duckdb_result], col: idx_t): CString = extern

  @name("wvlet_duckdb_column_type")
  def duckdb_column_type(result: Ptr[duckdb_result], col: idx_t): duckdb_type = extern

  @name("wvlet_duckdb_result_error")
  def duckdb_result_error(result: Ptr[duckdb_result]): CString = extern

  // Chunk / vector API for row iteration. The deprecated `duckdb_value_*` row-API was
  // neutered in DuckDB 1.5.2 (returns null/0), so the chunk API is the only working path.
  type duckdb_data_chunk = Ptr[Byte]
  type duckdb_vector     = Ptr[Byte]

  @name("wvlet_duckdb_destroy_data_chunk")
  def duckdb_destroy_data_chunk(chunk: Ptr[duckdb_data_chunk]): Unit = extern

  @name("wvlet_duckdb_data_chunk_get_size")
  def duckdb_data_chunk_get_size(chunk: duckdb_data_chunk): idx_t = extern

  @name("wvlet_duckdb_data_chunk_get_vector")
  def duckdb_data_chunk_get_vector(chunk: duckdb_data_chunk, col: idx_t): duckdb_vector = extern

  @name("wvlet_duckdb_vector_get_data")
  def duckdb_vector_get_data(vector: duckdb_vector): Ptr[Byte] = extern

  @name("wvlet_duckdb_vector_get_validity")
  def duckdb_vector_get_validity(vector: duckdb_vector): Ptr[ULong] = extern

  @name("wvlet_duckdb_validity_row_is_valid")
  def duckdb_validity_row_is_valid(validity: Ptr[ULong], row: idx_t): CBool = extern

  // The C API's `duckdb_fetch_chunk` takes `duckdb_result` BY VALUE, which Scala Native's
  // @extern ABI doesn't emit correctly for a 48-byte struct; the wrapper takes a pointer and
  // forwards by value on the C side.
  def wvlet_duckdb_fetch_chunk(result: Ptr[duckdb_result]): duckdb_data_chunk = extern

end DuckDBApi

/**
  * Finds and loads libduckdb once per process. Resolution order matches the Scala.js backend:
  *   1. `WVLET_LIBDUCKDB` env var (explicit override)
  *   2. The bare library name, so the OS loader searches `DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` /
  *      `PATH` and its default directories
  *   3. Well-known install paths for the current platform (Homebrew, /usr/local, …)
  */
private[duckdb] object DuckDBLoader:

  /**
    * The error from the first failed candidate: `WVLET_LIBDUCKDB` when set, since that is the
    * library the user asked for, else the loader's own search
    */
  private var firstError: Option[String] = None

  /** True when libduckdb was found and every binding in [[DuckDBApi]] resolved. */
  lazy val isLoaded: Boolean = candidatePaths.exists(tryLoad)

  def notAvailableMessage: String =
    val reason = firstError.map(e => s" (${e})").getOrElse("")
    "libduckdb is not available. Install DuckDB (e.g. `brew install duckdb`) or set " +
      s"WVLET_LIBDUCKDB to the absolute path of libduckdb${reason}"

  private def tryLoad(path: String): Boolean = Zone {
    if DuckDBApi.wvlet_duckdb_load(toCString(path)) == 0 then
      true
    else
      if firstError.isEmpty then
        firstError = Some(fromCString(DuckDBApi.wvlet_duckdb_load_error()))
      false
  }

  private def candidatePaths: List[String] =
    val envPath     = sys.env.get("WVLET_LIBDUCKDB").map(_.trim).filter(_.nonEmpty).toList
    val systemPaths =
      if LinktimeInfo.isMac then
        List(
          "libduckdb.dylib",
          "/opt/homebrew/lib/libduckdb.dylib",
          "/usr/local/lib/libduckdb.dylib"
        )
      else if LinktimeInfo.isWindows then
        List("duckdb.dll", "C:\\Program Files\\duckdb\\duckdb.dll")
      else
        List(
          "libduckdb.so",
          "/usr/local/lib/libduckdb.so",
          "/usr/lib/libduckdb.so",
          "/usr/lib/x86_64-linux-gnu/libduckdb.so",
          "/usr/lib/aarch64-linux-gnu/libduckdb.so"
        )
    envPath ++ systemPaths

end DuckDBLoader

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
