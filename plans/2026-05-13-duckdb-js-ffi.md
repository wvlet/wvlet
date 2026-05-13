# DuckDB on Node.js for the wvlet compiler — feasibility verdict

Date: 2026-05-13

## TL;DR

**Feasible. Recommended approach: bundle `libduckdb.{dylib,so,dll}` from DuckDB's
GitHub release + bind it via the [koffi](https://www.npmjs.com/package/koffi) FFI
library.** Synchronous from JS, ~5 ms per `schemaOf` warm, ~80 ms cold, no
N-API addon, no node-gyp, no async refactor of `TypeResolver`.

End-to-end PoC ran against a real parquet fixture and a JSON file on first try;
the bound C API surface is exactly the 9 functions we already bind from Scala
Native, and the type enum values match (e.g. `17 = VARCHAR`, `5 = BIGINT`).

## Why we can't use `@duckdb/node-api` directly

The entire `@duckdb/node-api` query/execution API is Promise-based. There is no
`runSync`/`querySync`/`prepareSync`. `TypeResolver.resolveLocalFileScan` calls
`DuckDBAnalyzer.guessSchema(path): RelationType` synchronously inside the typer
phase, and there is no clean async-to-sync bridge in modern Node (`deasync` is
unmaintained and unsafe; child process spawning blocks per call).

## Why bundling libduckdb + koffi works

[Investigated separately](https://github.com/duckdb/duckdb-node-neo):
`@duckdb/node-api` itself is **just an N-API addon over the synchronous C API**.
The Promise wrapper is a Node convention bolted on by running the sync C call
on a libuv worker thread and resolving a `Promise::Deferred`. The C API
(`duckdb_query` etc.) is the same one we already bind from JVM (JDBC →
duckdb_jdbc) and Scala Native (`@extern @link("duckdb")`). A direct FFI call
from JS hits the same sync code path with no async machinery underneath.

Going via the github release (`libduckdb-{linux-amd64,linux-arm64,osx-universal,
windows-amd64,windows-arm64}.zip`) instead of `@duckdb/node-bindings-*`:

- Same dylib byte-for-byte (verified — versions track 1:1, e.g. `1.5.2-r.1` ↔
  `v1.5.2`).
- Adds `duckdb.h` (npm package doesn't ship the header).
- Skips the useless 400 KB N-API addon.

## Measurements

Apple Silicon (M-series Mac), Node 25.8.0, libduckdb v1.5.2, koffi from npm.
PoC scripts under [`experiments/2026-05-13-duckdb-js-ffi/`](../experiments/2026-05-13-duckdb-js-ffi).

| Phase                              | Time   |
| ---------------------------------- | ------ |
| `koffi.load(libduckdb.dylib)`      | ~19 ms |
| Bind 9 C functions + result struct | <1 ms  |
| First `schemaOf` (cold parquet)    | ~80 ms |
| Warm `schemaOf` (avg of 100)       | ~5 ms  |

Reference points:
- Native `wvc compile` cold-start with the existing libduckdb binding: similar
  ballpark; once the runtime is up, schema queries are dominated by DuckDB engine
  init.
- JVM `wvlet compile` via JDBC: cold ~100 ms, warm comparable.
- Hypothetical `execSync duckdb` fallback: process spawn alone is ~40–50 ms
  even with a warm OS page cache. Not competitive for a typer that may resolve
  many file references in one compile.

## Architecture parity

Same `DuckDBApi` shape across all three platforms:

```
JVM    : org.duckdb.* (JDBC over JNI over libduckdb C API)
Native : @extern @link("duckdb")  → libduckdb C API  (already shipped, #1695)
JS     : koffi FFI to bundled libduckdb shared lib   ← proposed
```

The C API surface for `DuckDB.schemaOf` is the same 9 functions in all three
backends: `duckdb_open`, `duckdb_close`, `duckdb_connect`, `duckdb_disconnect`,
`duckdb_query`, `duckdb_destroy_result`, `duckdb_column_count`,
`duckdb_column_name`, `duckdb_column_type` (+ `duckdb_result_error` for
diagnostics). One mental model, three runtimes.

## Distribution plan (for the real PR)

| Concern              | Decision                                                                       |
| -------------------- | ------------------------------------------------------------------------------ |
| npm package          | `@wvlet/cli` declares `koffi` as a regular dep (28 MB unpacked, prebuilt all platforms — no node-gyp). |
| libduckdb shipping   | Ship per-platform via npm `optionalDependencies` — `@wvlet/libduckdb-darwin-arm64`, `@wvlet/libduckdb-linux-x64`, etc. Each wraps the matching github-release zip. ~35 MB compressed per platform. Only the target's binary installs. |
| Bundled lib lookup   | `DuckDBCompat.js` resolves the dylib path at runtime via `process.platform` + `process.arch`, then `koffi.load(absolutePath)`. Falls back to system-installed `libduckdb` if the bundled package isn't present. |
| Unsupported platform | If neither bundled nor system `libduckdb` is found, surface a clear error pointing to `https://duckdb.org/docs/installation/`. JSON files keep working (they go through `JSONAnalyzer`, no DuckDB needed). |
| CI                   | Add `npm install` of the bundled libduckdb in the test_js step, mirroring how `test.yml`'s test_native step now downloads libduckdb from the github release. |

## Implementation outline (next PR)

1. Convert `wvlet-lang/.js/src/main/scala/.../duckdb/DuckDBCompat.scala` from
   the current stub to a real implementation:
   - `@JSImport("koffi", JSImport.Namespace)` → koffi facade
   - Resolve libduckdb path: try `@wvlet/libduckdb-${platform}-${arch}` first,
     then `process.env.WVLET_LIBDUCKDB`, then system search
   - Mirror the `DuckDBApi` ↔ `DuckDBCompat.schemaOf` split we used on Native
2. Add `@wvlet/libduckdb-*` packages to `sdks/` (one per platform) that vendor
   the github-release zip. Could autogenerate from a small script.
3. Update `sdks/cli-node/package.json` with the optional deps + a postinstall
   message if no libduckdb is available.
4. CI: extend the npm publish workflow to also publish the libduckdb wrapper
   packages on tags.
5. Drop the "JS DuckDB unsupported" test skip in `DuckDBAnalyzerTest` — the
   parquet test should now pass on JS too, with `DuckDB.isAvailable` flipping
   to `true` when koffi successfully loads the bundled or system dylib.

## What I'm not committing in this PR

This is a feasibility branch only. Lands:

- `experiments/2026-05-13-duckdb-js-ffi/` — runnable spike (40 lines + benchmark).
- `plans/2026-05-13-duckdb-js-ffi.md` — this writeup.

The actual `DuckDBCompat.js` implementation, libduckdb npm-wrapper packages, and
CI provisioning will come in a follow-up PR after the user signs off on this
direction.
