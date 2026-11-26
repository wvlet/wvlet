# Estimate composite-key joins from declared unique keys

## Goals

- Tell a fact ⋈ fact join on two key columns that are no key of either side (TPC-DS q25 and q17: catalog_sales ⋈ store_returns on item and customer, 13 real rows, estimated 35K) from a join on a composite primary key (TPC-H q9: lineitem ⋈ partsupp on part and supplier; TPC-DS store_sales ⋈ store_returns on ticket and item).
- With that, q25 and q17 can pick DuckDB's join order (0.71x wall written by hand) while q9 keeps its plan.
- Lay the metadata path that a later PR uses to group by a key alone (TPC-H q10, TPC-DS q23, q47).

## Background

- `CardinalityEstimator.joinOutputRows` divides |L|·|R| by the product of each key pair's distinct count, capped at max(|L|, |R|), because the keys of one join are usually correlated (a composite primary key). That cap makes q25's two independent columns look like a key.
- Parquet footers can't tell the two cases apart: per-column distinct counts look the same, and samples of store_returns' (item, customer) pairs are as distinct as partsupp's primary key. The 2026-11-25 probe-cost attempt confirmed that.
- The engine already stores declared keys: `create table` with `primary key` / `unique` columns keeps them in `TableSpec.uniqueKeys` (the `wvlet.table-spec` Iceberg property of warehouse tables). Nothing in planning reads them.
- Plain Parquet files have no place to declare keys. TPC-H and TPC-DS document their primary keys.

## Design

Lane: Standard — a planner estimate change. Direction pre-set by the work item and the engine loop rules (merge after green CI and a no-regression SF10 A/B); no new syntax in this PR.

- `TableSource` gets `uniqueKeys`: column sets declared unique. Warehouse tables made by `create table` expose their `TableSpec` keys; `ParquetSource` and `MemTable` take them at registration (`EngineCatalog.registerParquet(name, path, uniqueKeys)`); the generated TPC-H tables declare the spec's primary keys.
- The documented TPC-H / TPC-DS primary keys live in one main-code table (`catalog/BenchmarkKeys`), used by `TpchGen` and by the test and bench harnesses when they register Parquet files.
- A join's keys between two inputs are classified from the scans they trace to (through filters, samples, column projections and hash joins):
  - covers a declared unique key of one side → the pair has that side's base rows as distinct values on both sides (FK → PK containment), so |L|·|R'| / |R| as for single-column foreign keys;
  - both sides' tables declare keys and neither side covers one → the columns are independent: the product of the per-column divisors without the max(|L|, |R|) cap (DuckDB's estimate);
  - otherwise (no declarations, a join input, an expression) → today's estimate.
- A second toggle `probe_scan_cost` charges each join with a one-input probe for the probe rows its build's runtime filters keep. Without it the corrected q25 estimate joins catalog_sales to store_returns before its date filter (SF10 1.35x); with it the search finds DuckDB's order. A build key's distinct count is capped by the rows of the build's sub-joins that hold its input (needed for q72).
- Behind a new `Optimization` toggle `key_join_estimates`, in the join enumerator, the greedy search and the aggregated-input estimate (the runtime-plan `JoinSearch`); the operator-level estimator keeps today's rule.
- Golden plans are regenerated (the plan tests register the benchmark keys); SF10 A/B on DS q25, q17, q29, q72, q64 and H q9, q18, q3 plus suite totals.
- Out of scope, recorded as follow-ups: grouping by a key alone with carried columns (H q10, DS q23, q47), foreign-key declarations, and a user-facing way to declare keys of Parquet files (coordinator request in the work item).

## Alternatives and Why Not?

- **Drop the cap for every composite key.** Regresses q9 and every store_sales ⋈ store_returns join, whose composite key the cap models correctly.
- **Composite distinct counts from samples.** Both cases look unique in samples; the difference is FK containment, which samples of independent row groups can't see.
- **A plan-time sample join.** Reads both tables' key columns per edge at plan time, with high variance at q25's 13 rows; declared keys are free and exact.
- **New syntax or a sidecar file for Parquet keys now.** User-facing; left for the coordinator's decision.
