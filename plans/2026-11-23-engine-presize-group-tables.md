# Engine: pre-size group tables and reuse per-batch scratch

## Goals

- Cut the allocation and copying of growing `GroupTable`s and aggregate state, which the SF1 profiles show in the aggregation-heavy TPC-DS queries.
- Change no results and add no planning cost visible in queries whose plans don't change.

## Background

- SF1 profiles (async-profiler, 2026-11-23): key store `grow`, `rehash` / `emptySlots`, and `Accumulator.ensureGroups` cost 4.2% of engine CPU in q78 and 3.4% in q67 under hash aggregation, and 1–1.8% in q49 / q78 under hash join builds. They cost almost nothing elsewhere.
- Every `GroupTable` starts at 1024 slots and 16 stored keys, then doubles. Each doubling copies every key column and refills and reinserts every slot. Accumulators double in the same way.
- Hash join builds know the most groups each partition can hold (its row count) but don't pass it to the table. Parallel builds also hash every build key twice: once to partition the rows and again inside each partition's table.
- Aggregations know nothing about their group count at run time. `Stats` has source distinct counts and row estimates, but it ignores filter selectivity, so its estimates run high.
- Decimal sums already run on unscaled longs with an overflow check (`SumDecimal`), so the work item's third point needs no change.

## Design

Lane: Express. This is an internal performance change with no change in results, and it touches no always-gate area.

- `GroupTable` takes an optional expected group count. It sizes its slots, its hash list, and its key stores to that count up front. Without a hint it starts small and grows, as it does today.
- **Join builds:** each partition's table is sized to its row count, which is an exact upper bound. Parallel builds pass the partitioning hashes to the table instead of hashing the keys again.
- **Aggregations:** `HashAggOp` carries an expected group count from the planner and pre-sizes both its table and its accumulators.
  - The estimate is the product of the keys' distinct counts from `Stats`, capped by the estimated input rows.
  - There is no hint when any key has no statistics.
  - A fixed cap (about 2^18 groups) bounds the cost of an overestimate, since `Stats` ignores filters.
  - The parallelizer keeps the hint for final and single-phase aggregations. Partitioned aggregations split it across partitions and cap each share by that partition's row count.
  - Partial (per-morsel) aggregations get no hint: the query-wide estimate would pre-size one table per Parquet row group. Sizing them needs each part's row count from `ScanSplits`, which is a follow-up.
- **Scratch reuse:** a table reuses its hash buffer only inside `groupIds`, where it is the only writer. `keyHashes` and `lookup` run across threads on built join tables, so they keep allocating. `HashAggOp` reuses its group-id array for aggregations without keys and its arrays for `FILTER` clauses.
- A new `Optimization` toggle turns the planner's group estimate off, for an SF1 A/B on one build. It is listed in `OptimizerOptionsTest`.
- Gates: TPC-H 22/22, TPC-DS 99/99, spec/basic conformance, and `GroupTableTest` cases for pre-sized tables (including a hint larger than the actual group count). Engine JS and Native must compile. The PR reports SF1 A/B numbers for q67, q78, q49, and the full TPC-DS total.

## Alternatives and Why Not?

- **Size aggregation tables from materialized input rows (in `PartitionedAggOp`):** it is exact only as an upper bound. When there are far fewer groups than rows, it wastes 16+ bytes per row.
- **Grow by 4x instead of 2x:** this cuts the number of copies, but it can over-allocate by up to 4x and doesn't use the information we already have.
- **Leave Parquet / gather allocation to other PRs:** they are bigger wins, but separate sessions own them (selection vectors, uni assembly).
