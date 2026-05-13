# `DuckDB.execute` on JS and Native — follow-up notes

Date: 2026-05-13

`DuckDB.execute(sql): QueryResult` landed for the JVM backend (via JDBC). JS and
Native intentionally still report `canExecute = false` and throw on `execute` for
distinct reasons captured here.

## Scala.js — ✅ landed

DuckDB 1.5.2 disabled the deprecated row-based value API (`duckdb_value_varchar`
etc. return null). The chunk/vector API is the only working path. Implemented
via koffi using the following pattern:

1. **Two-query approach**: schema probe (`SELECT * FROM (<sql>) ... LIMIT 0`)
   for original column types, then `SELECT COLUMNS(*)::VARCHAR FROM (<sql>)`
   for the data — every column is VARCHAR so we only need one chunk reader
   path.
2. **Per-row varchar unpacking**: each `duckdb_string_t` is 16 bytes — the
   first 4 are length, then either 12 inlined bytes (length ≤ 12) or a
   4-byte prefix + 8-byte heap pointer (length > 12).
3. **The trick for following the heap pointer**: `koffi.decode(data, offset, "void *")`.
   Using `"void *"` (rather than `"char *"`) keeps the inner pointer as a
   koffi pointer instead of auto-stringifying via NUL termination. Then a
   second `koffi.decode(innerPtr, koffi.array("uint8_t", length))` reads the
   exact byte range.
4. **Null check via the validity bitmap** (`duckdb_validity_row_is_valid`)
   with `KoffiOps.address(p) == js.BigInt(0)` to detect when the chunk has
   no validity bitmap (all valid).

`canExecute = true` on JS when koffi successfully loads libduckdb. Tests for
`execute` now run on JS automatically.

## Scala Native — struct-by-value ABI mismatch

`duckdb_fetch_chunk` takes the 48-byte `duckdb_result` struct **by value** in C
(`duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result)`). Scala Native's
`@extern` calling convention for `CStruct6[...]` parameters doesn't currently
emit the right ABI for this size — verified by a probe: `duckdb_fetch_chunk`
returns null on the first call even though the same C function works correctly
from JDBC (JVM) and koffi (JS), proving the C library itself is fine.

`duckdb_result_get_chunk(result, idx)` and `duckdb_result_chunk_count(result)`
also take the result by value, so they hit the same issue. There's no
pointer-taking variant in the public C API.

**Path of least resistance**: add a tiny C wrapper alongside `wvc-lib` that
takes the result by pointer and forwards by value to `duckdb_fetch_chunk`:

```c
duckdb_data_chunk wvlet_fetch_chunk(duckdb_result *r) { return duckdb_fetch_chunk(*r); }
duckdb_idx_t      wvlet_chunk_count(duckdb_result *r) { return duckdb_result_chunk_count(*r); }
```

`wvc-lib` already builds C-callable artifacts; adding a small `.c` file plus
the `extern` Scala Native bindings for these wrappers is the cleanest path.

Estimated effort: ~half a day.

## What landed in this iteration

- Shared `QueryResult` + `QueryResultRow` model
- Shared `QueryResultPrinter` (CSV + Unicode-box-drawn table, fully tested
  cross-platform: 3 tests passing on JVM/JS/Native)
- JVM `DuckDB.execute` via JDBC's `ResultSet`
- 5 cross-platform `DuckDBExecuteTest` tests, gated by `DuckDB.canExecute`:
  - `canExecute = true` on JVM → tests run
  - `canExecute = false` on JS/Native → tests cleanly `ignore`
- Pruned the broken deprecated-row-API bindings from the JS `DuckDBApi`

The plumbing for `wvlet run` from `WvletCli` is ready as soon as either the JS
or Native backend lands `execute`.
