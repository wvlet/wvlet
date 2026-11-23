# M5: Runtime join filters

## Goals

- A hash join gives the Parquet scans on its probe side the [min, max] range of its build keys once the build is done, so they skip row groups that can't match. DuckDB pushes join filters into scans the same way.
- Target: fact tables clustered by a join key. At SF1, `catalog_sales` is clustered by `cs_sold_date_sk` (12 row groups, each covering about 8% of the key range) and `inventory` by `inv_date_sk` (96 row groups, about 1% each). Joins with a filtered `date_dim` build can skip most of their row groups. `store_sales` and `web_sales` are not clustered: every row group spans the whole range, so nothing is skipped there.

## Background

- `PushScanFilters` gives a scan the row-group filter implied by the filter directly over it (`ScanFilters.toParquet`). A filter that only a join implies (the keys of a filtered dimension) never reaches the fact scan.
- A hash join builds before it probes: `HashJoinOp.next` builds first, and `MorselOp` runs the join builds of its pipeline (`prepare`) before it asks the source for its row groups (`scanSplits(scan.spec)`). So a filter set at the end of the build is in place when the probe scan starts.

## Design

- Lane: Express. An internal optimizer change behind a toggle, with no user-facing surface; merge is pre-authorized by the launch prompt.
- `RuntimeFilter` (exec): a slot for one build key and one scan column. `JoinBuild` sets it from the non-NULL build keys (min and max over `IntVec` / `LongVec`). `ScanOp.spec` ANDs the slots that are set with its static filter. A scan that starts before the build is done reads without the filter, which is still correct.
- `Lowering` decides where filters go, because the runtime plan still has column ids. A join gets one filter per key when:
  - the join drops unmatched probe rows: inner, semi, and right joins, or left / anti joins that build on the left (a right join or right anti join over swapped inputs);
  - the probe key is a plain column whose values pass unchanged from a filterable Parquet scan, through filters, plain projections, and the probe sides of joins that preserve their rows (either side of inner joins, the left side of semi / anti / left joins, the right side of right joins, whichever the join probes). A build side is read while building, which can come before the build above sets the filter (morsel pipelines prepare inner builds first), so it gets none. The path never crosses an aggregation, window, limit, union, or shared CTE read;
  - the build key has the same values: both integers, or both dates, or both timestamps.
- Dropping a probe-side scan row whose key is outside the range only drops rows that carry that key value up to the join, and the join drops those rows anyway. NULL keys never match, so the range (which excludes NULLs) keeps SQL semantics.
- The Parquet translation reuses `ScanFilters` (new `ScanFilters.range`, shared with `valuesFilter`), so only types whose Parquet statistics order matches the engine's order are filtered.
- Explain shows the filtered columns on the scan (`join_filter=...`). Golden plans change by that annotation only: no operator moves.
- Toggle `runtime_join_filters`.
- Tests: `RuntimeFilterTest` (shared; the range from the build keys), `EngineRuntimeJoinFiltersTest` (inner and semi joins set the probe scan's filter with unchanged results; left joins and scans below an aggregation get none; the toggle), plus golden plans and the conformance suites.

## Not in this PR

- IN-set filters (the build's distinct keys as equalities) for unordered types such as strings: `ScanFilters` keeps up to 64 equalities, so only tiny builds would qualify.
- Passing filters through aggregations on their group keys, and into shared CTE bodies.
- Filtering rows (not only row groups) in the scan by the range.

## Alternatives and Why Not?

- Decide on the physical operator tree after `Parallelizer`: the morsel pipelines are closures there, so the tree can't be walked down to the scan. The runtime plan is the last tree with column ids and visible structure.
- Wrap the build input in an operator that watches the keys flow by: the column pruner and the parallelizer would have to learn the new operator, and the keys would be evaluated twice.
