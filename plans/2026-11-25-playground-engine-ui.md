# Playground on the Wvlet engine with a uni-ui layout

## Goal

Turn the web playground into a syntax and function reference that runs: queries execute on Wvlet's columnar engine in the browser (no DuckDB-Wasm), the Wvlet→SQL translation and plans show next to the result, SQL converts to Wvlet, and the layout uses uni-ui components.

## Design

- Data: `TpchGen` in `wvlet-engine` generates TPC-H tables in memory for a scale factor (0.001 to 0.1 in the UI). See [the ADR](../adr/2026-11-25-playground-engine-tpch-gen.md).
- Logic: `PlaygroundEngine` (no DOM) compiles with a typed catalog (`EngineCatalog.toCompileCatalog`), selects the statement at the cursor (`QuerySelector`), defines the file's vals, runs the relation on `WvletEngine`, and returns SQL for a dialect, the Wvlet text of SQL input, the logical plan, the engine plan, and rows as strings.
- UI (uni-ui): header (Wvlet/SQL tabs, Run, Describe, TPC-H scale factor, theme, Share), resizable foldable sidebar (Examples tree with search, Tables tree), Monaco editor with the example's title and description, a resizable output pane (SQL / Plan / Engine tabs, dialect select), a vertical splitter, a sortable `DataTable` of results, and a status bar. Layout sizes and the theme persist in localStorage; the URL hash names the example or carries the query text.
- Examples: `src/main/wvlet/Examples/<NN_category>/*.wv|*.sql`, title and description from the leading comments; `PlaygroundEngineTest` runs all of them on Node.js.

## Follow-ups

- Engine gaps found by the examples: `select _1` after `group by` fails with `Column not found: _1`; `count_if(l_returnflag = 'R')` fails with "Could not convert 'R' to boolean". Add the examples back once fixed.
- Run queries in a Web Worker so SF 0.1 generation and long queries don't block the page.
- Expose `TpchGen` as a data source outside the playground (e.g., a `wvlet` connector option or a CLI command to write the tables to Parquet).
- Run `test` assertions and show their results.
