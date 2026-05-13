# `DuckDB.execute` on JS and Native — follow-up notes

Date: 2026-05-13

`DuckDB.execute(sql): QueryResult` landed for the JVM backend (via JDBC). JS and
Native intentionally still report `canExecute = false` and throw on `execute` for
distinct reasons captured here.

## Scala.js — koffi struct unpacking

**Why it doesn't work today**: DuckDB 1.5.2 disabled the deprecated row-based
value API (`duckdb_value_varchar`, `duckdb_value_int64`, etc.) — they return
`null` / `0` regardless of the actual cell. Verified via a koffi probe directly
calling the C functions: all return null in 1.5.2, while `duckdb_column_*`
metadata calls and the chunk API still work.

The replacement is the chunk/vector API:

- `duckdb_fetch_chunk(result)` → `duckdb_data_chunk`
- `duckdb_data_chunk_get_size(chunk)` → row count in chunk
- `duckdb_data_chunk_get_vector(chunk, col)` → vector for a column
- `duckdb_vector_get_data(vector)` → raw `void *` pointing to typed column data
- `duckdb_vector_get_validity(vector)` → `uint64_t *` validity bitmap
- `duckdb_validity_row_is_valid(validity, row)` → null check helper
- `duckdb_destroy_data_chunk(&chunk)` → cleanup

For varchar columns, the raw data is an array of 16-byte `duckdb_string_t`
unions (length-prefixed; either inlined ≤ 12 bytes or out-of-line via pointer).
`duckdb_string_t_data(struct *)` and `duckdb_string_t_length(struct)` are
helpers but the latter takes the struct by value.

**Path of least resistance**: wrap the user SQL with
`SELECT COLUMNS(*)::VARCHAR FROM (<sql>)` so every column is VARCHAR — one
chunk reader path instead of per-type dispatch (verified working with the
DuckDB CLI). Then read each row's `duckdb_string_t` via koffi.

**What got stuck**: koffi can read an array of structs via
`koffi.decode(data, koffi.array("duckdb_string_t", n))`, but the inline-vs-
pointer union is awkward to express in koffi's struct DSL. The cleanest way is
to define `duckdb_string_t` with `{length: uint32_t, raw: uint8_t[12]}` and
parse the 12 bytes manually based on `length` — for the out-of-line case
that means rebuilding a koffi pointer from a BigInt address, which doesn't
have a direct API (`koffi.as(bigint, "uint8_t *")` rejects BigInts).

Working approaches (pick one in the follow-up PR):
1. Read the whole vector as a `Uint8Array` view via `koffi.decode(data, koffi.array("uint8_t", n*16))` and reconstruct strings entirely in JS (for the out-of-line case, the 8-byte pointer at offset 8 needs to be passed back to libduckdb somehow — `duckdb_string_t_data` can be called with the 16-byte struct address since it handles both cases).
2. Wrap the C API surface (specifically the chunk-readout for one row) in a tiny `.node` addon, or use [napi-rs](https://napi.rs/) style.
3. Use the `apache-arrow` npm package + `duckdb_query_arrow` to get an Arrow IPC stream and parse it in JS — already a dep in the playground build.

Estimated effort: ~half a day if going with option 1, longer for 2 or 3.

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
