# Engine: parallel outer-join tails and smaller outer builds (TPC-DS q75)

## Problem
- TPC-DS q75 joins each channel's filtered sales (1.07M store_sales rows at SF10) LEFT to its returns (2.9M store_returns). The engine builds on the returns and scans every returns row; DuckDB builds on the sales and its dynamic filter cuts the catalog_returns scan to 579K of 1.44M rows.
- Building on the preserved side makes the join a RIGHT join, whose unmatched build rows `MorselOp` emitted once, on the calling thread, after every part. A RIGHT join's tail also kept a UNION ALL input out of the union's pipeline, so `union_distinct_once` fell back to per-input distincts. #2474 measured the swap at 2.25x slower wall.
- The swap also never fired: q75's `d_year = 2002 or d_year = 2001` comes from the CTE readers after `ReorderJoins` chose build sides, and an `or` of equalities was estimated at the default 0.3.

## Change (three toggles)
- `parallel_outer_tail`: the tail of a pipeline with one right / full join runs as tasks over disjoint ranges of the build rows (`HashJoinOp.withTailRange`, `MorselOp.Tail`). UNION ALL inputs with tails join the union's pipeline; their tails run after every part of the union.
- `smaller_outer_builds` (needs `parallel_outer_tail`): outer joins swap their build at 2x instead of 4x. A shared CTE body re-chooses outer build sides after the readers' filters are pushed (`ReorderJoins.outerBuilds`).
- `or_equals_selectivity`: `x = a or x = b` is estimated as n / ndv, like `x in (a, b)`.

## Not done
- Runtime filters derived from a LEFT join's probe side into its build scan would need the probe read before the build. Building on the preserved side gives the same filter through the existing probe-side runtime filters.
