# ADR: TPC-DS tables are generated to dsdgen's shape from the queries' literals, not dsdgen's distributions

Date: 2026-11-26 · PR #2472

## Context

The engine generates TPC-H tables while they're scanned ([the playground ADR](2026-11-25-playground-engine-tpch-gen.md)), and `prepareTPCH` on a `wvlet` connector registers them. TPC-DS had no such source: its data came only from DuckDB's `dsdgen`. A port of `dsdgen` would reproduce its values, but its value lists (`tpcds.dst`: names, cities, colors, categories, carriers, ...) fall under the TPC EULA, and many of the 99 queries filter on exact literals (`i_category = 'Women'`, `s_store_name = 'ese'`, `ca_county in (...)`, `i_brand in (...)`), so generic random data would leave most queries empty. Plan: [2026-11-26-tpcds-engine-gen](../plans/2026-11-26-tpcds-engine-gen.md).

## Decision

- Match `dsdgen`'s shape, measured from DuckDB's output, not its values:
  - Tables the specification fully defines (`date_dim`, `time_dim`, `customer_demographics`, `household_demographics`, `income_band`) are formulas of the row index and equal `dsdgen`'s row by row, including its calendar quirks (1900 counted as a leap year, day-of-year holidays and quarters, Friday weekends). Only `d_last_dom` differs.
  - The other tables have `dsdgen`'s columns and types, row counts, dense keys, NULL shares, slowly changing dimension layout, and order structure: per-order customer, demographics, address, store / call center / warehouse; distinct items per ticket; returns matching their sale; price identities; seasons; weekly inventory of current item revisions.
- Take values from the TPC-DS query literals (categories with the classes the queries pair them with, brands, states, counties, cities, carriers, zip codes, the digit syllables of names) plus generic filler, never from `dsdgen`'s distribution file.
- Gate the result by query shape: `TpcdsGenQueriesTest` runs every spec/tpcds query on generated and `dsdgen` tables at SF 0.1 and fails when a query returns rows on `dsdgen`'s but not on the generated ones, outside a `knownMisses` list that only shrinks (q04, q23, q29 at first, one row each on `dsdgen`'s).
- Cap the scale factor at 10: `dsdgen`'s row counts were measured from SF 0.01 to 10 (linear between its SF 1 and 10 counts, rounded to per-table units), and SF 100 is too large to generate locally to check.

## Consequences

- `prepareTPCDS` / `tpcdsScaleFactor` on a `wvlet` connector give TPC-DS data with no DuckDB or files, and the same profile works on DuckDB. A connector can register TPC-H or TPC-DS, not both (both have `customer`).
- Answers differ from the official ones and from `dsdgen`'s, so the engine benchmarks, golden plans, and `EngineTPCDSTest` keep reading `dsdgen` Parquet.
- A query that needs a correlation or literal the generator lacks shows up as a `TpcdsGenQueriesTest` failure; fix it in the generator, not by adding to `knownMisses`.
- Scale factors above 10 need `dsdgen`'s counts measured first.
