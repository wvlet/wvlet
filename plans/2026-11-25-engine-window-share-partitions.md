# Engine: share window partitions across calls with the same window

- Goal: window calls with the same PARTITION BY and ORDER BY in one `WindowOp` (TPC-DS q51's outer `max`/`max`) partition and sort the rows once instead of once per call.
- Profile at SF10 (async-profiler): window is ~40% of q51 engine CPU, ~11% of q57, ≤6% of q47/q12/q98. Whole-partition aggregates (`avg(x) over (partition by ...)`) are 0.6% of q47 and 1.6% of q57 CPU: they are already computed once per partition, so a GROUP BY + join rewrite isn't worth it now.
- Design: `WindowOp.callGroups` groups calls by `(partitionBy, orderBy)` compared with `==` (not `sig`, which omits row-function closures). `evaluateGroup` partitions/sorts once per group (sequential and hash-parted parallel paths) and runs every call's `evalRows` on the same partitions. `topNOf` already requires one window for all calls, so top-N still applies.
- Toggle: `window_share_partitions`, a `WindowOp` flag set at lowering and carried by Parallelizer and ColumnPruner. Plans don't change.
- Tests: WindowOpTest compares shared vs unshared over interleaved windows (sequential, parallel, top-N); EngineWindowTopNTest checks the flag wiring and the grouping after lowering.
