# ADR: Hash joins give their probe-side scans the range of their build keys

Date: 2026-11-23 · PR (M5, runtime join filters)

## Context

A Parquet scan skips row groups by the filter directly over it (`PushScanFilters`, `ScanFilters.toParquet`). A filter that only a join implies never reaches the scan: a fact table joined with a filtered `date_dim` reads every row group, even when the fact table is clustered by date. At SF1, TPC-DS `catalog_sales` is clustered by `cs_sold_date_sk` (12 row groups, each covering about 8% of the key range) and `inventory` by `inv_date_sk` (96 row groups, about 1% each). The framework plan (section 4) lists runtime join filters as the other cheap adaptive step of M5, after DuckDB's join filter pushdown.

## Decision

- A hash join's build sets, for each eligible key, the [min, max] range of its non-NULL build keys as a row-group filter (`RuntimeFilter`) on the probe-side scan that reads that key. `ScanOp.spec` ANDs the filters that are set with its own filter.
- The filter is set when the build is done, before the probe side starts: `HashJoinOp` builds before it pulls the probe, and `MorselOp` runs the builds of its pipeline before it asks the source for its row groups. A scan that started earlier reads without the filter, and the result is the same: the filter only skips data.
- `Lowering` decides where filters go, because the runtime plan still has column ids and its structure. After `Parallelizer`, the morsel pipelines are closures and the tree can't be walked down to the scan.
- A join gets a filter only when dropping a probe row whose key is outside the build's range can't change the result:
  - The join drops unmatched probe rows: inner, semi and right joins, and left and anti joins that build on the left (a right join and a right anti join over swapped inputs). Left, full and anti joins that probe with their preserved side get none.
  - The probe key is a plain column whose value passes unchanged from the scan row to the join: through filters, plain projections, and the probe sides of joins that preserve their rows (either side of an inner join, the left side of semi, anti and left joins, the right side of right joins, whichever the join probes). A build side is read while building, which can come before the build above it sets the filter: a morsel pipeline prepares its inner builds first. The path never crosses an aggregation, window, limit or shared CTE read. It does follow each input of a UNION ALL (added in a follow-up PR): each output row is one input row with the key unchanged, so every input scan gets its own filter, except an input whose key column is cast to a wider type. Each row at the join then carries the key of the scan row it came from, so dropping that scan row only drops rows the join would drop.
  - The build key holds the same kind of values as the probe column: integers, dates, or timestamps. `ScanFilters` translates only types whose Parquet statistics order matches the engine's order.
- NULL keys never match a hash join, and the range excludes NULLs, so the filter keeps SQL semantics.
- `explain` shows the filtered columns on the scan (`join_filter=...`), so plans show where filters go. `runtime_join_filters` turns it off.

## Consequences

- Golden plans change by the `join_filter` annotation only (114 of 121): no operator moves.
- At SF1, TPC-DS q21 runs 0.25–0.34× and q72 about 0.6–0.7× (both over `inventory`, by engine min). Queries over unclustered fact tables (`store_sales`, `web_sales`) skip nothing and are neutral within noise. Full-suite geomean is 0.954 on/off, against an A/A of 0.875–1.10.
- Only ranges are used (superseded for integer keys by the per-row bitmap and Bloom filters of [2026-11-24](2026-11-24-engine-row-level-join-filters.md)). IN-set filters (the build's distinct keys) for strings and other unordered types, filters through aggregations on their group keys, and filters into shared CTE bodies are left out.
- Follow-up (UNION ALL inputs): of the remaining ideas, only filters into union inputs skip row groups at SF1 (q05 and q54 read `catalog_sales` through a union under a filtered `date_dim` join). IN-set filters skip nothing (no string column's row-group ranges differ, and tables with string join keys have one row group), and no SF1 plan needs filters through aggregation group keys, onto build sides, or into shared CTE bodies (see plans/2026-11-23-engine-optimizer-m5-union-join-filters.md).
