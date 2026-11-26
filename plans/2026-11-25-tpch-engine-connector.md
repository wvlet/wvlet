# TPC-H tables on the Wvlet engine without DuckDB

## Goal

Give the in-process engine (`type: wvlet`) a built-in TPC-H data source, as DuckDB's `tpch` extension does, so demos, tests, and `wv` sessions get TPC-H tables with no DuckDB, no data files, and no setup step. Generate the data during the scan, in independent parts, so scale factors above 1 work without holding the whole dataset in memory, and parts generate in parallel.

## Non-goals

- Matching the reference `dbgen` values. `TpchGen`'s values differ (see [the playground ADR](../adr/2026-11-25-playground-engine-tpch-gen.md)), so answers differ from the official answer sets. The engine benchmarks (`EngineTPCHBench`), golden plans (`spec/engine-plans/`), and DuckDB-oracle tests keep reading `dbgen` Parquet.
- TPC-DS. It gets its own plan once this lands (see [TPC-DS later](#tpc-ds-later)).

## Current state

- `TpchGen` (`wvlet-engine/src/main/scala/wvlet/lang/engine/catalog/TpchGen.scala`) builds the 8 tables as `MemTable`s at scale factor ≤ 1. Each table draws from one seeded `Random` stream over all its rows, so a row range can't be generated alone. `orders` and `lineitem` come out of one pass (`ordersAndLineitems`).
- Only the playground (`PlaygroundEngine`) and `TpchGenQueriesTest` use it.
- `TableSource` already has what a generated source needs: `scanSplits` (independent parts), `estimatedRows`, `columnRange`, `partRanges`, `estimatedDistinct`, and `key`.
- `WvletEngineSession` registers tables from the `tables`, `dataDir`, and `warehouse` properties. A source named `s.t` lands in schema `s`. `register` adds a `MemTable` or `ParquetSource` schema to the compile-time catalog, but no other source type.
- The DuckDB connector takes `prepareTPCH`/`tpchScaleFactor` (and `prepareTPCDS`/`tpcdsScaleFactor`) and runs `call dbgen(...)` into `main`. `RunnerSpec` and `TPCHBench` set these properties. `wvlet ui --tpch` is documented as "DuckDB only".

## Design

### 1. Generate in blocks (`TpchGen`)

- Cut each table into fixed-size blocks: 10,000 rows for `supplier`, `customer`, and `part`; 10,000 orders (with their 1 to 7 line items each) for `orders`/`lineitem`; `partsupp` follows `part` blocks (4 rows per part). Seed each block's `Random` from (table, block index), so a block generates on its own and the output is still the same for a given scale factor.
- Each block returns a `Batch` of the requested columns. The first version generates every column and selects the requested ones. Skipping unrequested string columns (comments, names, addresses dominate the cost) is a follow-up, done only if profiling shows the gain.
- `orders` and `lineitem` blocks share one generator (line items derive from their order). Scanning both tables generates each order block twice; that costs less than caching across scans.
- Values change from today's output (the seeds move to blocks). That's fine: no test pins exact generated values beyond row counts and `TpchGenQueriesTest`'s non-empty results. Re-check those.
- `TpchGen.tables(sf)` stays as the materialized API for the playground: it concatenates the blocks. Keep the browser cap there (the playground UI caps the scale factor itself); the streaming source has no cap.

### 2. `TpchSource extends TableSource` (wvlet-engine, all platforms)

- `name`, `schema` (as today, types matching DuckDB's Parquet files), `estimatedRows` from the spec row counts (lineitem: the generator's exact count when cheap, otherwise 6,000,000 × sf).
- `canSplit = true`; `scanSplits` returns one part per block with `rows`. `scan` runs the parts in order.
- Estimates from the spec, no sampling: `columnRange`/`partRanges` for keys (dense keys per block; order keys are sparse but their range per block is known) and dates (order date range; ship, commit, and receipt dates are order date + fixed offsets). `estimatedDistinct` for keys, flags, and enum columns. These feed join ordering and range-filter estimates the way Parquet footers do.
- `key = ("tpch", sf, table)`, so plans over the same table share work.
- `TpchGen.register(catalog, sf, schema)` registers the 8 sources. Use it from `TpchGenQueriesTest` too.
- Engine planning: this is a new source, not a planner change, so no `Optimization` toggle. Plans over `ParquetSource` stay unchanged and the golden plans don't move.

### 3. Connector properties (`WvletEngineSession`)

- `prepareTPCH: true` and `tpchScaleFactor: <sf>` (default 0.01, as DuckDB's) register the 8 tables in the connector's default schema. These are the same property names as the DuckDB connector, so one profile and `RunnerSpec(prepareTPCH = true)` work on both engines.
- `register` also adds a `TpchSource`'s schema to the compile-time catalog (it's cheap, like `MemTable`), so stdlib methods such as `o_comment.like(...)` resolve.
- A name clash with a `tables`, `dataDir`, or `warehouse` table is an error, as clashes between those already are.
- `prepareTPCDS` on a `wvlet` connector throws `INVALID_ARGUMENT` ("TPC-DS generation isn't supported on the wvlet engine yet") rather than being ignored.

### 4. CLI and server

- `wvlet ui --tpch`: use the same property on the engine connector, and drop "(DuckDB only)" from the option description and `website/docs/usage/ui.md`.
- `wv` and `wvlet run` get no new flag: the profile property is the switch (same choice as [the TPC-H DuckDB robustness plan](2026-09-12-tpch-duckdb-robustness.md)). Revisit if users ask for `wv --tpch`.
- Optional, separate PR: `wvlet tpch --sf 1 -o <dir>` to write the tables as Parquet with `DataFileWriter`, for sharing a dataset without DuckDB. Mark the output as not `dbgen`-identical.

## Tests

- `TpchGenTest` (shared, so JVM and JS): spec row counts at sf 0.01 and 0.1; `scanSplits` parts concatenated equal `scan`; the same block generated twice is equal; projection returns the requested columns in order; `columnRange`/`estimatedDistinct` bound the actual values.
- `TpchGenQueriesTest`: keep all 22 queries running and the non-empty checks, now through `TpchSource`. Add one run at sf 1 on the JVM to show streaming works (time it; drop it to a bench if it takes over ~20 s).
- `WvletEngineSession` test: a profile with `prepareTPCH` and `tpchScaleFactor` sees the 8 tables, typed, and rejects `prepareTPCDS`.
- `RunnerSpecTPCH` stays on DuckDB. Add `RunnerSpecTPCHEngine`, which runs spec/tpch on the `wvlet` engine with `prepareTPCH`, checking that queries run rather than their answers. spec/tpch has no `test` assertions, so every file can run.
- Run `langJS/test` and `engineJS/test`, and check that the playground still loads (relink `playground/fastLinkJS` first).

## Docs

- `website/docs/usage/` engine connector page: the `prepareTPCH`/`tpchScaleFactor` properties, with a note that the data isn't `dbgen`-identical.
- `website/docs/for-ai-agents.md` and `plugins/wvlet/skills/wvlet/`: the new connector option (`SkillSnippetTest`/`SkillFreshnessTest`).
- `design/connectors.md`: built-in generated sources of the `wvlet` connector. Update the playground ADR's last consequence when this lands ("not done yet").

## PR order

1. Block generation + `TpchSource` + tests (engine only, no user-facing change).
2. `prepareTPCH` on the `wvlet` connector, `wvlet ui --tpch`, `RunnerSpecTPCHEngine`, docs.
3. Optional: the `wvlet tpch` Parquet writer command.

## TPC-DS later

- 24 tables. `dsdgen` draws from many distribution files (categories, classes, brands, states, counties, colors, sizes, date and time dimensions, slowly-changing dimension rows with `rec_start_date`/`rec_end_date`).
- Many of the 99 queries filter on exact literals (`i_category = 'Women'`, specific states, years, `d_moy`, manager ids). A generator that doesn't keep those domains returns empty results, which say little about plans.
- The target would be a `TpcdsGen` that keeps the value domains the spec/tpcds queries use, checked by a test that most queries return rows at sf 0.1. It wouldn't promise answers matching the official ones. Estimate: several times the TPC-H work, so it gets its own plan.
