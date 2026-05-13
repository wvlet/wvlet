// Feasibility experiment: call libduckdb directly from Node via koffi (sync FFI).
//
// Setup:
//   1. `brew install duckdb` (provides /opt/homebrew/lib/libduckdb.dylib)
//   2. `npm install koffi`
//   3. `node experiment.mjs`
//
// What it proves: can we open an in-memory DuckDB, run a probe query, and read
// column names + types without touching @duckdb/node-api's Promise API at all.

import koffi from "koffi";

// 1. Load libduckdb. On macOS Homebrew puts it at /opt/homebrew/lib; on Linux
//    /usr/local/lib (per our test.yml install). koffi.load() returns a library handle
//    we can `func()` against. Throws synchronously if the path doesn't resolve.
const libduckdb = koffi.load("/opt/homebrew/lib/libduckdb.dylib");

// 2. Opaque pointer types. We never inspect the inside of these — DuckDB hands them
//    back and we hand them in.
const duckdb_database = koffi.opaque("duckdb_database");
const duckdb_connection = koffi.opaque("duckdb_connection");

// 3. duckdb_result struct layout. All fields below are deprecated per duckdb.h, but
//    the struct still has to match in size and alignment for the stack allocation
//    `duckdb_query` writes into. Six fields: three uint64 (idx_t), then a pointer
//    to deprecated_columns, then a CString error_message, then internal_data.
const duckdb_result = koffi.struct("duckdb_result", {
  deprecated_column_count: "uint64_t",
  deprecated_row_count: "uint64_t",
  deprecated_rows_changed: "uint64_t",
  deprecated_columns: "void *",
  deprecated_error_message: "char *",
  internal_data: "void *",
});

// 4. Bind the C API surface. koffi uses `_Out` / `_Inout` to mark pointer parameters
//    that DuckDB writes into. Return types use the C signatures verbatim (duckdb_state
//    is just `int`).
const duckdb_open = libduckdb.func(
  "int duckdb_open(const char *path, _Out_ duckdb_database **out_database)"
);
const duckdb_close = libduckdb.func(
  "void duckdb_close(_Inout_ duckdb_database **database)"
);
const duckdb_connect = libduckdb.func(
  "int duckdb_connect(duckdb_database *database, _Out_ duckdb_connection **out_connection)"
);
const duckdb_disconnect = libduckdb.func(
  "void duckdb_disconnect(_Inout_ duckdb_connection **connection)"
);
const duckdb_query = libduckdb.func(
  "int duckdb_query(duckdb_connection *connection, const char *query, _Out_ duckdb_result *out_result)"
);
const duckdb_destroy_result = libduckdb.func(
  "void duckdb_destroy_result(_Inout_ duckdb_result *result)"
);
const duckdb_result_error = libduckdb.func(
  "const char *duckdb_result_error(duckdb_result *result)"
);
const duckdb_column_count = libduckdb.func(
  "uint64_t duckdb_column_count(duckdb_result *result)"
);
const duckdb_column_name = libduckdb.func(
  "const char *duckdb_column_name(duckdb_result *result, uint64_t col)"
);
const duckdb_column_type = libduckdb.func(
  "int duckdb_column_type(duckdb_result *result, uint64_t col)"
);
const duckdb_library_version = libduckdb.func(
  "const char *duckdb_library_version()"
);

// 5. Driver: open in-memory DB, run a probe query, list columns, clean up. All sync.
function schemaOf(filePath) {
  const dbBox = [null];
  if (duckdb_open(null, dbBox) !== 0) throw new Error("duckdb_open failed");
  const db = dbBox[0];
  try {
    const conBox = [null];
    if (duckdb_connect(db, conBox) !== 0) throw new Error("duckdb_connect failed");
    const con = conBox[0];
    try {
      const result = {};
      const sql = `select * from '${filePath.replace(/'/g, "''")}' limit 0`;
      if (duckdb_query(con, sql, result) !== 0) {
        const err = duckdb_result_error(result);
        duckdb_destroy_result(result);
        throw new Error(`duckdb_query failed: ${err}`);
      }
      try {
        const n = duckdb_column_count(result);
        const columns = [];
        for (let i = 0n; i < n; i++) {
          columns.push({
            name: duckdb_column_name(result, i),
            type: duckdb_column_type(result, i),
          });
        }
        return columns;
      } finally {
        duckdb_destroy_result(result);
      }
    } finally {
      duckdb_disconnect([con]);
    }
  } finally {
    duckdb_close([db]);
  }
}

// 6. Run it. Use one of the parquet fixtures that ships in the wvlet repo.
console.log("libduckdb version:", duckdb_library_version());

const repoRoot = "/Users/leo/work/wvlet/.claude/worktrees/glowing-swimming-moore";
const parquetPath = `${repoRoot}/spec/cdp_behavior/data/weblog_users_first_purchased_at/part-00000-90048009-4be7-472e-ad1a-019aed6bd6fa-c000.snappy.parquet`;

const t0 = process.hrtime.bigint();
const cols = schemaOf(parquetPath);
const t1 = process.hrtime.bigint();
console.log(`schemaOf("${parquetPath}"):`);
for (const c of cols) console.log(`  ${c.name}: type#${c.type}`);
console.log(`Elapsed: ${Number(t1 - t0) / 1e6}ms`);

// Quote-handling smoke test
const tricky = `${repoRoot}/spec/basic/person.json`;
console.log("\nperson.json columns:");
for (const c of schemaOf(tricky)) console.log(`  ${c.name}: type#${c.type}`);
