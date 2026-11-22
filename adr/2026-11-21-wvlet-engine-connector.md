# ADR: The `wvlet` connector type runs the in-process engine

Date: 2026-11-21 · PR #2202

## Context

`wvlet-engine` ([ADR](2026-11-21-columnar-engine.md)) executes typed Wvlet plans directly, but the CLI, REPL, and runner only knew SQL engines. The connector type name `wvlet` was already taken in the cross-platform CLI: `-t wvlet --host <h>` sends the query to a remote wvlet server. Without a host that path only failed with "host is required".

## Decision

- A connector of type `wvlet` **without a host** runs queries on the in-process engine; **with a host** it stays a remote wvlet server. `WvletEngineSession.isInProcess` is the single check. A separate type name (e.g. `engine`) was rejected: `wvlet` is the natural name for Wvlet's own engine, and the host already tells the two apart without breaking existing profiles.
- The engine is not a SQL engine, so it is not a `DBConnector`. `PlanExecutor` (cross-platform) and `QueryExecutor` (JVM) check the active connector and hand the `Relation` to `WvletEngineSession.execute` instead of generating SQL. SQL statements (DDL, `execute`) on this connector fail with NOT_IMPLEMENTED instead of being sent anywhere.
- Tables are registered from connector properties (`dataDir`, `tables`) and Parquet / in-memory table schemas also go into an `InMemoryCatalog` so the compiler sees them (JSON / CSV schemas need a full parse, so they stay untyped until scanned). `save to <table>` stores an in-memory `MemTable`; other write statements are not supported yet.
- A new `DBType.Wvlet` gives the compiler a dialect for this connector. It selects DuckDB stdlib function variants, because the engine implements DuckDB function semantics and is conformance-tested against DuckDB.

## Consequences

- `wvlet run -t wvlet`, the `wv` REPL, the server, and the npm/native CLI can query Parquet / JSON / CSV / Iceberg data with no DuckDB.
- `RunnerSpecWvletEngineTPCH` and `RunnerSpecWvletEngineBasic` run the spec suites (`test` assertions included) on the engine through the runner. Their ignore lists only shrink as the engine adds features.
- The JVM runner rejects a remote `wvlet` connector (one with a host); only the cross-platform CLI talks to remote servers.
