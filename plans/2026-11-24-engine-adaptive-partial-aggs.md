# Engine: skip partial aggregation that doesn't reduce its input

## Goals

- Cut TPC-DS q78 (SF10 2.1x DuckDB) and other GROUP BYs of near-unique keys (q67's ROLLUP sets, q75), where each morsel's partial aggregate inserts every row into a hash table and saves the final aggregate nothing.

## Background

- q78 groups each channel by (year, item, customer): 4.97M store rows into 4.90M groups at SF10 (DuckDB count), so partial reduction is 0.99. async-profiler (3 runs): the partial aggregates take 10% of engine CPU and the final partitioned aggregate 20%, regrouping the same rows.
- DuckDB's partial aggregation abandons a hash table that doesn't reduce its input and sends rows on.
- Parts (Parquet row groups after filters) hold about 21K rows in q78, so a per-part decision at 64K rows would never fire.

## Design

Lane: Standard (an executor change behind a toggle); within the launch prompt's loop.

- New toggle `adaptive_partial_aggs` (on by default). `TwoPhaseAgg.plan` gives every partial `HashAggOp` of one aggregation a shared `PartialAggSkip`.
- Grouping stops once at least 16,384 rows were seen (summed over finished parts, or within one part) and at least 90% of them were kept as groups. The decision is never reverted. A stopping part emits its groups so far, then each remaining batch as one-row partial results (accumulators over identity group ids, so filters, NULLs, and types match the grouped path); parts starting later pass every batch through.
- Only partial aggregates (whose output is aggregated again) take the flag. Final groups and values don't change; the order of output groups and of floating-point additions can, as parts finish in any order. Results are compared as multisets.
- Bound: at a 0.9 ratio after 16K rows, a part has at least ~80K distinct keys, so a wrong stop loses at most about a 1.5x reduction of that part.
- Tests: `PartialAggSkipTest` (shared): the thresholds, and the final aggregate over passed-through partials (count(*), filtered count / sum, Int64 and Decimal sum, min, max, with NULLs and repeated keys) equals the grouped one; low-cardinality input keeps grouping. `EngineAdaptivePartialAggsTest` checks the flag reaches the partial aggregates. `EngineOptimizerToggleTest` ignores it (plans don't change).
