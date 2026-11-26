# ADR: Estimate composite-key joins from declared unique keys, not footer statistics

Date: 2026-11-25 · PR #2461

## Context

- The join search estimates an equi-join as |L|·|R| divided by each key pair's distinct count, with the product capped at max(|L|, |R|), because the keys of one join are usually one correlated key (TPC-DS store_sales ⋈ store_returns on ticket and item; TPC-H lineitem ⋈ partsupp on part and supplier).
- TPC-DS q25 and q17 join catalog_sales ⋈ store_returns on item and customer, two independent columns that are no key of either table. The cap estimates 35K rows where there are 13, so the engine can't choose the small build that lets store_sales skip row groups (DuckDB's order is 0.71x wall).
- Per-column footer statistics and row-group samples look the same for both cases; what tells them apart is whether the columns are a key of a side, i.e. metadata.
- The engine already keeps declared keys: `create table` stores `primary key` / `unique` columns in `TableSpec.uniqueKeys` (the `wvlet.table-spec` property of warehouse tables). Plain Parquet files have no place for them.

## Decision

- `TableSource.uniqueKeys` exposes declared unique column sets. Warehouse tables read them from their spec property; `ParquetSource`, `MemTable` and `EngineCatalog.registerParquet` take them from the caller; generated TPC-H tables declare the spec's primary keys. `catalog/BenchmarkKeys` holds the documented TPC-H and TPC-DS primary keys for harnesses that register their Parquet files.
- With `key_join_estimates`, keys of several pairs that each trace to one scan on each side are estimated from those declarations:
  - covering a declared key of one side (base rows B) → a foreign key to that primary key, divisor B (capped by each side's rows, as for one key);
  - no declared key of either side, both declaring some → independent columns, the product of the pairs' divisors without the cap;
  - anything else (no declarations, expressions, columns of several scans) → the previous estimate.
- With the estimate alone, q25's catalog_sales ⋈ store_returns looks so small that the search joins it before catalog_sales' date filter, so catalog_sales is read in full (SF10 1.35x wall). `probe_scan_cost` therefore lands with it: each join with a one-input probe also costs the probe rows its build's runtime filters keep (the smallest build / probe distinct ratio over the keys), with a build key's distinct values capped by the rows of every join in the build tree holding its input, since an inner join adds no key values. Without that cap, a date_dim build multiplied by a week join looked unselective and q72 ran 2.4x slower.
- A table that declares any unique key is taken to declare all of them (closed world), which is what makes "covers none" meaningful.
- Keys of plain Parquet files will come from a sidecar keys file (`<file>.keys.json`, or `_keys.json` in the folder) with single and composite unique keys and optionally foreign keys, not new DDL syntax: the user's decision (2026-11-25), implemented in a follow-up PR.

## Consequences

- `key_join_estimates` alone changes TPC-DS q17, q25, q29 and q85; with `probe_scan_cost`, 25 golden plans change (TPC-H q8, q9, q10). SF10 one-JVM A/B of both: q25 0.81x wall / 0.58x CPU, q17 0.83x CPU, q29 0.89x CPU, q72 0.88x wall; suites flat (TPC-DS wall 1.00 / CPU 0.98, TPC-H wall 0.95 / CPU 1.02).
- Probes that are joins pay no scan charge, so a large single input can become the build: TPC-H q10 builds on customer instead of probing it with a join filter (CPU about 1.15x). Charging both sides symmetrically is a follow-up.
- Benchmarks give the engine the specifications' keys and DuckDB none; DuckDB's estimator multiplies per-column divisors without a cap, so it gets q25 right without them.
- A wrong declaration (a key that isn't unique) misestimates joins but can't change results, since only join order and build sides read it.
- The same metadata is the basis for grouping by a key alone with dependent columns carried (TPC-H q10, TPC-DS q23, q47), a follow-up.
