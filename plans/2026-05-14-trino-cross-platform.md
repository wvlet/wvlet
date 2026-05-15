# Cross-platform Trino client (JVM / Node.js / Native)

Date: 2026-05-14

## Context

After the cross-platform `DuckDB.execute` work landed (#1705/#1706/#1707), `wvlet run` could compile + execute against DuckDB from JVM, Node.js, and Native. Trino was still JVM-only via `trino-jdbc` in `wvlet-runner`, leaving the Node and Native paths without a way to talk to a real production warehouse.

Trino's [client protocol](https://trino.io/docs/current/develop/client-protocol.html) is pure HTTP — no native driver required. uni already shipped a sync HTTP client on JVM (Apache HttpClient) and Native (libcurl). The missing piece was sync HTTP on Node.js, which was the only platform with no built-in synchronous HTTP primitive.

That gap was closed upstream in [wvlet/uni#546](https://github.com/wvlet/uni/pull/546) — a `NodeSyncHttpChannel` built on `worker_threads` + `Atomics.wait` + `SharedArrayBuffer`. Once uni 2026.1.10 published, all three platforms had a working `HttpSyncClient`, unblocking the cross-platform Trino client.

## Direction

A pure-HTTP `Trino` facade that mirrors `DuckDB`'s shape:

```
wvlet-lang/
  src/main/scala/wvlet/lang/compiler/analyzer/trino/
    Trino.scala         -- object Trino.execute(sql, config): QueryResult
    TrinoConfig.scala   -- host/port/user/catalog/schema/source + builders
  .{jvm,js,native}/src/main/scala/wvlet/lang/compiler/analyzer/trino/
    TrinoCompat.scala   -- per-platform Http.setDefaultChannelFactory init
  .jvm/src/test/scala/wvlet/lang/compiler/analyzer/trino/
    TrinoTest.scala     -- in-process fake Trino over com.sun.net.httpserver
```

`Trino.execute` drives the protocol:

1. POST `/v1/statement` with the SQL text + `X-Trino-{User,Source,Catalog,Schema}` headers.
2. Walk `nextUri` GETs until the response has no `nextUri`.
3. Accumulate `columns` (only carried on the first response that has results) and `data` rows into a `QueryResult` whose row shape (`Seq[Option[String]]`) is identical to what `DuckDB.execute` returns — so `QueryResultPrinter.toBox` / `toCsv` work unchanged.

## What landed

| Module              | Change                                                                                         |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| `build.sbt`         | `UNI_VERSION` 2026.1.9 → 2026.1.10 (Node sync HTTP)                                            |
| `wvlet-lang`        | New shared `Trino` + `TrinoConfig` + per-platform `TrinoCompat` (channel factory registration) |
| `wvlet-lang/.jvm`   | `TrinoTest` — in-process fake Trino server, exercises columns/rows/pagination/error paths      |
| `wvlet-cli-core`    | `wvlet run -t trino --host ... --catalog ...` — `--target` now drives both SQL dialect and runtime backend |

`HttpSyncClient` is the only DB I/O primitive — no JDBC, no platform-specific drivers, no async-to-sync bridging in user code. The same `Trino.execute` source runs unchanged on:

- **JVM** — uni `JVMHttpChannelFactory` (Apache HttpClient under the hood)
- **Node.js** — uni `JSHttpChannelFactory` → `NodeSyncHttpChannel` (worker_threads + Atomics.wait)
- **Native** — uni `NativeHttpChannelFactory` (libcurl)

## Design decisions

### Why a per-platform `TrinoCompat` for HTTP init

uni auto-registers a channel factory inside `HttpCompat`, but that object only loads when the exception classifier needs it — which is too late if `Trino.execute` runs before any error path. The reproduction is `NoOpChannelFactory.newChannel` throwing "No HttpChannel implementation available" the first time you call `Http.client.newSyncClient`. Two ways to fix this:

1. **Explicit init by the caller** (what `wvlet-server` does — `Http.setDefaultChannelFactory(JVMHttpChannelFactory)`). Cross-platform code can't do this because the factory is per-platform.
2. **Per-platform init trait that `Trino` mixes in**. The shared `Trino` object calls `installHttpFactory()` once at class-init; each platform's `TrinoCompat` knows which factory to register. Mirrors how `DuckDBCompat` provides per-platform behavior.

We went with (2) so callers (including the CLI) don't have to think about it. It's idempotent — uni's `setDefaultChannelFactory` just reassigns the var.

### Why reuse `QueryResult` from the `duckdb` package

`QueryResult(columns: Seq[NamedType], rows: Seq[QueryResultRow])` and `QueryResultRow(values: Seq[Option[String]])` are already neutral — there's nothing DuckDB-specific about them. Moving them to a `wvlet.lang.compiler.runtime` package would be cleaner long-term but is a bigger refactor; for now Trino just imports them from `compiler.analyzer.duckdb`. Worth revisiting when a third backend appears.

### Why string-coerce all Trino values

Trino returns JSON values: `JSONString`, `JSONLong`, `JSONDouble`, `JSONBoolean`, `JSONNull`, and `JSONArray`/`JSONObject` for structured columns. The DuckDB backends already return values as `Option[String]` via `rs.getString` / `duckdb_value_varchar`. We string-coerce Trino values to match that surface so downstream printers don't have to branch on backend type. Structured columns fall back to `JSONValue.toJSON` — good enough for a CLI printer, replaceable later if we need typed access.

### Why `StatusCode.QUERY_EXECUTION_FAILURE` for Trino errors

`StatusCode.QUERY_EXECUTION_FAILURE` (a `UserError`) already exists and fits both the Trino `error` block (SQL syntax, semantics, runtime errors) and transport-level oddities (non-2xx, empty body, non-JSON body) — those all surface as "your query didn't execute." The exception message carries Trino's `errorName` / `message` so downstream callers can render the actual diagnostic.

The first cut of this PR used `NOT_IMPLEMENTED` as a placeholder; Gemini's review flagged it because `NOT_IMPLEMENTED` maps to HTTP 501 and misleads on what failed. Fixed before merge.

### Why a JVM-only smoke test instead of three platform suites

Cross-platform tests need an HTTP server to fake the Trino coordinator. uni's HTTP server (`uni-netty`) is JVM-only. The Trino test uses `com.sun.net.httpserver.HttpServer` from the JDK — quick to spin up on an ephemeral port and validates the full protocol loop (POST, paginate GETs, error block). JS/Native get coverage transitively via uni's own `HttpSyncClient` test suites — the only thing that's not exercised end-to-end on those platforms is the Trino protocol logic, which is plain-Scala JSON walking.

### Why `--target` drives both dialect and backend

The CLI's existing `--target` flag was documented as "always duckdb for `run`" — but `WvletCliCompiler` already used `--target` to choose the SQL dialect for code generation. Splitting backend from dialect would let users compile Trino SQL and execute it on DuckDB, which is rarely what's wanted. Coupling them makes `wvlet run -t trino ...` do the obvious thing without an extra flag.

## Open work

- **Real Trino integration test** — a docker-compose Trino + sample data, gated on an env var so it doesn't run in normal CI. Useful for catching protocol drift across Trino versions.
- **`--profile` support** — `wvlet-runner`'s JVM CLI already has `--profile <name>` reading from the profile config; the cross-platform `run` should learn the same so users don't have to repeat `--host --catalog --schema` for known profiles.
- **Auth** — currently only `X-Trino-User`. Basic / JWT / OAuth need to be added before this is usable against a production cluster.
- **Cancellation** — `DELETE /v1/statement/{queryId}/{nextToken}` can cancel a running query; we currently can't surface ctrl-C.
