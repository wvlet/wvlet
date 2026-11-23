/*
 * Runtime loader for libduckdb's C API.
 *
 * wvlet's native binaries do not link libduckdb: commands that never touch DuckDB must start on
 * machines without it. `wvlet_duckdb_load(path)` opens libduckdb with dlopen (LoadLibrary on
 * Windows) and resolves the functions wvlet calls; each `wvlet_duckdb_*` wrapper below forwards
 * to the resolved function. Callers must check that `wvlet_duckdb_load` succeeded before calling
 * any wrapper.
 *
 * The wrappers take `duckdb_result *` where the C API takes `duckdb_result` by value: Scala
 * Native's @extern ABI does not pass 48-byte structs by value correctly.
 *
 * The DuckDB types are declared here instead of including duckdb.h, so building wvlet needs no
 * DuckDB headers. The layouts follow duckdb.h's stable C API.
 *
 * Scala Native compiles every .c file under src/main/resources/scala-native/ into the consumer
 * binary. Bound from Scala by `DuckDBApi`.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

typedef uint64_t idx_t;
typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_data_chunk;
typedef void *duckdb_vector;
typedef int duckdb_state;
typedef int duckdb_type;

/* Same layout as duckdb.h's `duckdb_result` (the deprecated fields still occupy their slots) */
typedef struct {
  idx_t deprecated_column_count;
  idx_t deprecated_row_count;
  idx_t deprecated_rows_changed;
  void *deprecated_columns;
  char *deprecated_error_message;
  void *internal_data;
} duckdb_result;

/* X(return type, C API function name, parameter list) for every function wvlet resolves */
#define WVLET_DUCKDB_FUNCTIONS(X)                                                        \
  X(duckdb_state, duckdb_open, (const char *path, duckdb_database *out))                 \
  X(void, duckdb_close, (duckdb_database * db))                                          \
  X(duckdb_state, duckdb_connect, (duckdb_database db, duckdb_connection *out))          \
  X(void, duckdb_disconnect, (duckdb_connection * con))                                  \
  X(duckdb_state, duckdb_query,                                                          \
    (duckdb_connection con, const char *sql, duckdb_result *out))                        \
  X(void, duckdb_destroy_result, (duckdb_result * result))                               \
  X(idx_t, duckdb_column_count, (duckdb_result * result))                                \
  X(const char *, duckdb_column_name, (duckdb_result * result, idx_t col))               \
  X(duckdb_type, duckdb_column_type, (duckdb_result * result, idx_t col))                \
  X(const char *, duckdb_result_error, (duckdb_result * result))                         \
  X(duckdb_data_chunk, duckdb_fetch_chunk, (duckdb_result result))                       \
  X(void, duckdb_destroy_data_chunk, (duckdb_data_chunk * chunk))                        \
  X(idx_t, duckdb_data_chunk_get_size, (duckdb_data_chunk chunk))                        \
  X(duckdb_vector, duckdb_data_chunk_get_vector, (duckdb_data_chunk chunk, idx_t col))   \
  X(void *, duckdb_vector_get_data, (duckdb_vector vector))                              \
  X(uint64_t *, duckdb_vector_get_validity, (duckdb_vector vector))                      \
  X(bool, duckdb_validity_row_is_valid, (uint64_t * validity, idx_t row))

#define WVLET_DECLARE_FN(ret, name, params) \
  typedef ret(*name##_fn) params;           \
  static name##_fn p_##name = NULL;
WVLET_DUCKDB_FUNCTIONS(WVLET_DECLARE_FN)

static void *wvlet_duckdb_handle = NULL;
static char wvlet_duckdb_error[512] = "";

static void wvlet_duckdb_set_error(const char *prefix, const char *detail) {
  const char *parts[2] = {prefix, detail};
  size_t i = 0;
  for (int p = 0; p < 2; p++) {
    for (const char *s = parts[p]; s != NULL && *s != '\0' && i + 1 < sizeof(wvlet_duckdb_error);
         s++) {
      wvlet_duckdb_error[i++] = *s;
    }
  }
  wvlet_duckdb_error[i] = '\0';
}

static void *wvlet_duckdb_open_library(const char *path) {
#ifdef _WIN32
  void *h = (void *)LoadLibraryA(path);
  if (h == NULL) wvlet_duckdb_set_error("LoadLibrary failed: ", path);
  return h;
#else
  void *h = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (h == NULL) wvlet_duckdb_set_error("", dlerror());
  return h;
#endif
}

static void *wvlet_duckdb_symbol(void *h, const char *name) {
#ifdef _WIN32
  return (void *)GetProcAddress((HMODULE)h, name);
#else
  return dlsym(h, name);
#endif
}

static void wvlet_duckdb_close_library(void *h) {
#ifdef _WIN32
  FreeLibrary((HMODULE)h);
#else
  dlclose(h);
#endif
}

/* Returns 0 on success (or when libduckdb is already loaded), non-zero on failure */
int wvlet_duckdb_load(const char *path) {
  if (wvlet_duckdb_handle != NULL) return 0;
  void *h = wvlet_duckdb_open_library(path);
  if (h == NULL) return 1;
#define WVLET_RESOLVE_FN(ret, name, params)                         \
  p_##name = (name##_fn)wvlet_duckdb_symbol(h, #name);              \
  if (p_##name == NULL) {                                           \
    wvlet_duckdb_set_error("libduckdb is missing symbol: ", #name); \
    wvlet_duckdb_close_library(h);                                  \
    return 2;                                                       \
  }
  WVLET_DUCKDB_FUNCTIONS(WVLET_RESOLVE_FN)
  wvlet_duckdb_handle = h;
  return 0;
}

/* The reason the last failed wvlet_duckdb_load call failed */
const char *wvlet_duckdb_load_error(void) { return wvlet_duckdb_error; }

duckdb_state wvlet_duckdb_open(const char *path, duckdb_database *out) {
  return p_duckdb_open(path, out);
}
void wvlet_duckdb_close(duckdb_database *db) { p_duckdb_close(db); }
duckdb_state wvlet_duckdb_connect(duckdb_database db, duckdb_connection *out) {
  return p_duckdb_connect(db, out);
}
void wvlet_duckdb_disconnect(duckdb_connection *con) { p_duckdb_disconnect(con); }
duckdb_state wvlet_duckdb_query(duckdb_connection con, const char *sql, duckdb_result *out) {
  return p_duckdb_query(con, sql, out);
}
void wvlet_duckdb_destroy_result(duckdb_result *result) { p_duckdb_destroy_result(result); }
idx_t wvlet_duckdb_column_count(duckdb_result *result) { return p_duckdb_column_count(result); }
const char *wvlet_duckdb_column_name(duckdb_result *result, idx_t col) {
  return p_duckdb_column_name(result, col);
}
duckdb_type wvlet_duckdb_column_type(duckdb_result *result, idx_t col) {
  return p_duckdb_column_type(result, col);
}
const char *wvlet_duckdb_result_error(duckdb_result *result) {
  return p_duckdb_result_error(result);
}
duckdb_data_chunk wvlet_duckdb_fetch_chunk(duckdb_result *result) {
  return p_duckdb_fetch_chunk(*result);
}
void wvlet_duckdb_destroy_data_chunk(duckdb_data_chunk *chunk) {
  p_duckdb_destroy_data_chunk(chunk);
}
idx_t wvlet_duckdb_data_chunk_get_size(duckdb_data_chunk chunk) {
  return p_duckdb_data_chunk_get_size(chunk);
}
duckdb_vector wvlet_duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col) {
  return p_duckdb_data_chunk_get_vector(chunk, col);
}
void *wvlet_duckdb_vector_get_data(duckdb_vector vector) {
  return p_duckdb_vector_get_data(vector);
}
uint64_t *wvlet_duckdb_vector_get_validity(duckdb_vector vector) {
  return p_duckdb_vector_get_validity(vector);
}
bool wvlet_duckdb_validity_row_is_valid(uint64_t *validity, idx_t row) {
  return p_duckdb_validity_row_is_valid(validity, row);
}
