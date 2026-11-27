# Wvlet engine as an embedded library

## Goals

- Let an application run Wvlet queries in-process on wvlet-engine, the way it embeds DuckDB or SQLite, instead of only compiling them to SQL and shipping the SQL to another engine.
- Reach every SDK language through one shared Scala surface, so the JVM, TypeScript, C, and Python bindings are thin layers over the same code and return the same result shape.
- Ship it as a series of small PRs, each with its own public API, docs, and DuckDB conformance tests, with no planner changes.

## Background

- The SDKs only compile today: Python and libwvlet expose `compile_query` / `compile_query_json` (`wvc/.../WvcLib.scala`), and the npm `@wvlet/wvlet` package exposes `WvletCompiler`. The bindings page lists "Java/Scala" as a planned SDK.
- Running a query already works end to end in `wvlet-runner`: a `wvlet` connector with no host routes statements to `WvletEngineSession`, which registers tables (`tables`, `dataDir`, `prepareTPCH`), keeps a compile-time catalog of their schemas, and handles `save to`, `create table`, `append`, and `test`. `RunnerSpecWvletEngine*` runs spec/basic and spec/tpch this way.
- `wvlet-runner` and `wvlet-engine` are cross-built for JVM, Scala.js, and Scala Native, and the playground already runs the engine in the browser. An embedding API is packaging and a stable surface, not a new engine.
- Today embedding means assembling `Compiler`, `Profile`, `SqlConnectorProvider`, `PlanExecutor`, and `ExecutionPlanner` by hand (as `PlanExecutorWvletEngineTest` does). That is internal API.
- The JVM CLI types registered tables at compile time by handing the session's catalog to the compiler (`ConnectorCatalogs`, JVM-only). The shared code that JS and Native use has no such wiring.

## Design

Lane: Standard — a new public API. The launch prompt fixed the direction (design note, then the smallest surface first) and holds the merge.

### User flow (all languages)

- Open an engine, optionally with a base folder for relative paths.
- Register tables by name from files or folders: Parquet, JSON, CSV, Iceberg, Hive-partitioned Parquet. Files referenced in a query (`from 'data/x.parquet'`) need no registration.
- Run a query string. Statements run in order (`save to`, `create table`, `test` included); the result is the last query's rows, or an empty result when the text has no query (only `save to`, say).
- A compile error, a failed statement, or a failed `test` throws (JVM) or comes back as an error object (C, JSON) with the `StatusCode` and source location, the same as `compile_query_json` errors.
- Read the result: column names and types, rows, or JSON. Close the engine to release it.

### Shared surface

- One cross-platform class in `wvlet-runner`'s shared code, `wvlet.lang.embedded.WvletEmbed`, wraps a `WvletEngineSession` and a `PlanExecutor` on it. JS and Native get it for free, and later bindings only marshal arguments and results.
- The facade hands the session's compile-time catalog to its compiler in shared code, so registered Parquet tables are typed when the query compiles on every platform, as the JVM CLI does today.
- Queries return all rows; no 40-row display limit.
- One engine is one session: tables saved with `save to` stay visible to later queries. Calls on one engine are serialized; apps open one engine per thread for parallel queries.

### Result format

- Columns: name plus the Wvlet type name (`int`, `long`, `double`, `string`, `decimal[p,s]`, `date`, ...), by position: columns that share a name (`select a, b as a`) keep both values.
- Values are the runner's external values (`ValueOps.toExternal`): integers as Long, doubles as Double, decimals, dates, and timestamps as strings so no precision is lost, NULL as null.
- JSON first: `{"columns":[{"name":..,"type":..}],"rows":[[...],...]}`, rows as arrays. The C API and the TypeScript SDK return this same document, so one decoder per language suffices.
- Arrow later: the Arrow C Data Interface from libwvlet (zero-copy into pyarrow / pandas), and Arrow IPC for JS. JSON stays as the fallback.

### Order of PRs

1. **JVM / shared API (this PR):** `WvletEmbed` with `open`, `registerTable`, `query`, `close`, a result with Java-friendly accessors and `toJson`; a Java usage test; DuckDB conformance over spec/tpch data; a "Java / Scala" bindings page; a design page (`design/embedding.md`) owning the API and result contract, and an ADR for placing the facade in the runner.
2. **libwvlet C API:** `wvlet_engine_open(config_json)` returning a handle, `wvlet_engine_register_table`, `wvlet_engine_query_json`, `wvlet_engine_close`, `wvlet_free_string`, through the lazy-start C shim. Strings cross the boundary as UTF-8: today's `toCString` truncates each UTF-16 char to a byte, so it gets fixed here. Gate: `wvc-lib` `make test` with a non-ASCII string, and the full Native workflow matrix.
3. **Python SDK:** `wvlet.connect()` returning an engine with `register_table`, `run(query)` → rows, `.to_pandas()`; on the native library, no CLI fallback for running.
4. **TypeScript SDK:** a `WvletEngine` class in `@wvlet/wvlet` on the Scala.js build, for Node.js (files through uni.io) and browsers (tables registered from in-memory rows). `sdkJs` links only `lang.js` today, so this adds a second linked bundle on `runner.js` (or Scala.js module splitting), exported as a separate entry point so `WvletCompiler` users don't download the engine.
5. **Arrow output:** Arrow C Data Interface in libwvlet and pyarrow in Python; Arrow IPC in JS.
6. **Docs:** the website top page's Embedded panel (#2510) once the Python or TypeScript run API lands, since the panel speaks to app developers in those languages; the bindings index lists each binding as it lands.

### Gates for each PR

- DuckDB conformance: the binding's results equal DuckDB's for a subset of spec/tpch (and spec/basic where it reads files).
- Native PRs: `wvcLib/nativeLink`, `make test` in `wvc-lib`, `gh workflow run native.yml` on all platforms.
- No CLI or syntax change, so `for-ai-agents.md` and the bundled skill stay as they are unless a PR adds one.

## Alternatives and Why Not?

- **A public API on `WvletEngine` in wvlet-engine.** The engine executes typed relations only; compiling, table registration with typed schemas, `save to`, and `test` live in the runner. Duplicating them would drift from the CLI.
- **libwvlet run entry point first.** Its JSON result needs the same shared facade anyway, and the Native workflow makes each iteration slow; landing the facade on the JVM first lets PR 2 be marshalling only.
- **Run queries through the CLI (`wv`/`wvc`) from SDKs.** A process per query, no shared session, and text output to parse.
- **Expose `PlanExecutor` as-is.** It asks callers for a profile, a connector provider, and compile contexts, which are internal and change often.
