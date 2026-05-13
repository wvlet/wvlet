package wvlet.lang.compiler.analyzer.duckdb

import scala.scalajs.js
import scala.scalajs.js.annotation.*
import scala.util.Try

/**
  * Scala.js bindings to libduckdb's C API via [[Koffi]]. Mirrors the Scala Native bindings in
  * `.native/.../DuckDBApi.scala` — same 9 functions, same type enum.
  *
  * Library lookup is lazy: the first `instance` call probes a few well-known paths plus the
  * `WVLET_LIBDUCKDB` env var. If none resolve, `instance` returns `None` and the JS DuckDB backend
  * reports `isAvailable = false`. JSON files keep working because `DuckDBAnalyzer` routes them
  * through `JSONAnalyzer` higher up.
  */
private[duckdb] class DuckDBApi private (libPath: String):

  private val lib: KoffiLib = Koffi.load(libPath)

  // Declare the duckdb_result struct layout so koffi can size stack allocations correctly
  // when functions take `_Out_ duckdb_result *`. Fields below `internal_data` are all
  // deprecated per duckdb.h, but their slots still have to be present.
  Koffi.struct(
    "duckdb_result",
    js.Dynamic
      .literal(
        deprecated_column_count = "uint64_t",
        deprecated_row_count = "uint64_t",
        deprecated_rows_changed = "uint64_t",
        deprecated_columns = "void *",
        deprecated_error_message = "char *",
        internal_data = "void *"
      )
  )

  // Bound functions. Stored as `js.Function` and invoked via `.call(thisArg, args*)`; output
  // pointers are passed as single-element `js.Array` instances per koffi convention.
  val duckdb_open: js.Function = lib.func(
    "int duckdb_open(const char *path, _Out_ void **out_database)"
  )

  val duckdb_close: js.Function   = lib.func("void duckdb_close(_Inout_ void **database)")
  val duckdb_connect: js.Function = lib.func(
    "int duckdb_connect(void *database, _Out_ void **out_connection)"
  )

  val duckdb_disconnect: js.Function = lib.func("void duckdb_disconnect(_Inout_ void **connection)")
  val duckdb_query: js.Function      = lib.func(
    "int duckdb_query(void *connection, const char *query, _Out_ duckdb_result *out_result)"
  )

  val duckdb_destroy_result: js.Function = lib.func(
    "void duckdb_destroy_result(_Inout_ duckdb_result *result)"
  )

  val duckdb_result_error: js.Function = lib.func(
    "const char *duckdb_result_error(duckdb_result *result)"
  )

  val duckdb_column_count: js.Function = lib.func(
    "uint64_t duckdb_column_count(duckdb_result *result)"
  )

  val duckdb_column_name: js.Function = lib.func(
    "const char *duckdb_column_name(duckdb_result *result, uint64_t col)"
  )

  val duckdb_column_type: js.Function = lib.func(
    "int duckdb_column_type(duckdb_result *result, uint64_t col)"
  )

end DuckDBApi

private[duckdb] object DuckDBApi:

  /** Cached load attempt — populated lazily on first access. */
  private lazy val cached: Option[DuckDBApi] = tryLoad()

  /** Real [[DuckDBApi]] if libduckdb is loadable on this Node runtime, otherwise `None`. */
  def instance: Option[DuckDBApi] = cached

  private def tryLoad(): Option[DuckDBApi] = candidatePaths
    .iterator
    .flatMap { p =>
      Try(new DuckDBApi(p)).toOption
    }
    .nextOption()

  /**
    * Resolution order:
    *   1. `WVLET_LIBDUCKDB` env var (explicit override)
    *   2. Well-known absolute paths for the current platform/arch
    *   3. Bare `libduckdb` so the OS loader falls back to its own search path
    */
  private def candidatePaths: List[String] =
    val process     = js.Dynamic.global.process
    val env         = process.env
    val envPath     = Option(env.WVLET_LIBDUCKDB).filterNot(js.isUndefined).map(_.toString)
    val platform    = process.platform.toString
    val arch        = process.arch.toString
    val systemPaths =
      platform match
        case "darwin" =>
          arch match
            case "arm64" =>
              List("/opt/homebrew/lib/libduckdb.dylib", "/usr/local/lib/libduckdb.dylib")
            case _ =>
              List("/usr/local/lib/libduckdb.dylib", "/opt/homebrew/lib/libduckdb.dylib")
        case "linux" =>
          List(
            "/usr/local/lib/libduckdb.so",
            "/usr/lib/libduckdb.so",
            "/usr/lib/x86_64-linux-gnu/libduckdb.so",
            "/usr/lib/aarch64-linux-gnu/libduckdb.so"
          )
        case "win32" =>
          List("C\\:Program Files\\duckdb\\duckdb.dll")
        case _ =>
          Nil
    envPath.toList ++ systemPaths ++ List("libduckdb")

end DuckDBApi

/**
  * Mirror of `enum DUCKDB_TYPE` from `duckdb.h`. Only the codes we map to wvlet types are named;
  * everything else falls through to `DataType.AnyType` in `DuckDBCompat`. Identical to the Native
  * binding.
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
