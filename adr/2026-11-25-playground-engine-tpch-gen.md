# ADR: The playground runs on the Wvlet engine over TPC-H tables generated in the browser

Date: 2026-11-25

## Context

The web playground compiled Wvlet to SQL and ran it on DuckDB-Wasm, which downloads tens of megabytes of WebAssembly, runs `dbgen` inside it, and executes the generated SQL rather than Wvlet plans. The playground should instead show Wvlet itself: run on Wvlet's own columnar engine (which already cross-builds to Scala.js), cover the syntax as a runnable reference, and show the SQL translation and plans next to the result. The engine had no data in the browser: it reads Parquet through uni's file system, and TPC-H data came only from DuckDB's `dbgen` in tests and benchmarks.

## Decision

- The playground (`wvlet-ui-playground`) depends on `engine.js` and `uni-ui`, not DuckDB-Wasm. `PlaygroundEngine` compiles a query with the `wvlet` dialect, prints the logical plan and the engine's physical plan, and runs the statement at the cursor with `WvletEngine.execute`. The SQL view compiles the same text again with a catalog of the chosen dialect (DuckDB, Trino, Snowflake, Hive, BigQuery), since `Context.dbType` comes from the catalog.
- `TpchGen` (in `wvlet-engine`, all platforms) generates the eight TPC-H tables as `MemTable`s for a scale factor: spec row counts, sparse order keys, 4 suppliers per part, the retail-price formula, dates and flags derived from the order date, and column types matching DuckDB's Parquet files. Values are deterministic per scale factor but not those of the reference `dbgen`, so answers differ from the official ones. SF 0.01 (60K line items) generates in about 150 ms in the browser.
- `EngineCatalog.toCompileCatalog` gives the compiler the generated tables' column types. Without them, stdlib methods on columns such as `o_comment.like(...)` don't resolve.
- Examples live under `src/main/wvlet/Examples/<NN_category>/` as `.wv` and `.sql` files whose leading comment lines are the title and description. `PlaygroundEngineTest` runs each one on the engine (Node.js), so an example that stops compiling or running fails CI.
- The UI uses uni-ui components (`FoldablePane` + `Splitter`, `Tree`, `TabList`, `DataTable`, `Select`) styled by `uni-ui.css`, which sbt-uni copies from the jar on every compile; Tailwind is gone from the playground.

## Consequences

- The playground needs no network after load and shows only what the engine supports. Forms the engine can't run yet (`_1` grouping-key references in `select`, `count_if` on the engine) are left out of the examples until it does.
- Queries run on the main thread; a scale factor above 0.1 would block the page, so the UI offers 0.001 to 0.1 (`TpchGen` allows up to 1).
- `TpchGen` also backs the `wvlet` connector's `prepareTPCH` tables, generated block by block while scanned ([the plan](../plans/2026-11-25-tpch-engine-connector.md)).
