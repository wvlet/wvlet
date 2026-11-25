# Engine: sort window partitions by a primitive key

## Goals

- Cut TPC-DS q51's window partition sort: after #2387, `IndexSort.sort` under `WindowOp.partitions` is 18% of engine CPU at SF10 (async-profiler, 3 runs), plus 2% in a boxed `groups.max`.

## Background

- q51's three running windows order each item's partition (about a year of days) by `d_date`. `IndexSort` merge-sorts row indexes through `SortOp.comparator`, which re-reads both rows' NULL flags and values from the whole input column at each compare (cache misses: `IntVec.nulls` alone is 6% self time).
- The input is not grouped by item: the aggregation below is hash-partitioned by (item, date), so a presorted fast path doesn't apply; the sort itself has to get cheaper.

## Design

Lane: Standard (an executor change behind a toggle); within the launch prompt's scope and loop, merge held for your decision.

- New toggle `window_sort_kernels` (on by default): a partition ordered by one Int32, Date, or Int64 key reads each row's key once, packs (key code, position) into a long, and sorts with `Arrays.sort(long[])`. The code is the offset from the partition's min (max when descending), with NULLs coded below or above every value; ties keep their positions, so the order equals the stable `IndexSort` order.
- Other key types, several ORDER BY keys, or a key range that doesn't fit beside the position bits fall back to `IndexSort`.
- `groups.max` becomes a loop (no boxing), regardless of the toggle.
- Plans don't change: `EngineOptimizerToggleTest` ignores the toggle; `EngineWindowTopNTest` checks the flag reaches the window operators, `WindowSortKernelTest` (shared, JVM and JS) checks the order against `IndexSort` over Int32 / Date / Int64 keys with NULLs, ties, both directions and NULL placements, integer limits, and the fallbacks.
- Measure: SF10 q51 and the other window queries with `WVLET_ENGINE_BENCH_AB=window_sort_kernels`, both SF1 suites.
