# Engine: filter an aggregate's input by the keys its join can match

## Goals

- Follow-up to #2360 / #2362 for the TPC-H SF10 multi-way joins: q17 is the largest remaining gap (5.6–7x DuckDB at SF10, CPU ~6x).
- One focused fix: an aggregate that a join probes by its group key computes every group, though the join only ever looks up a few keys.

## Background

Profiled at SF10 on main cc65e118 (async-profiler `cpu,threads`, one JVM per query, warm-up plus 3 runs, load average 15–20):

- q17's plan: `lineitem ⋈ σ(part)` (about 2K parts) left-joins `avg(l_quantity) group by l_partkey` over all of `lineitem` (60M rows, 2M groups), on `p_partkey = l_partkey`. The aggregate is the decorrelated scalar subquery.
- The engine threads' CPU in q17 is the aggregation: `GroupTable.findLong` 16%, `insert` 9%, `assignIds` 4%, `SumDecimal.update` 4%, `LongVec.gather` 7%. All of it computes groups that the left join never looks up: only the ~2K keys of the filtered `part` can match.
- DuckDB pushes the outer keys into the subquery (a delim join), so it aggregates only the matching `lineitem` rows.
- The same shape: TPC-H q2 (`min(ps_supplycost)` per part) and q20 (`sum(l_quantity)` per part and supplier); TPC-DS q1 / q30 / q81 (`avg` per store or state) and q32 / q92 (`avg` discount per item).
- Not picked this time:
  - q21's zeroing is mostly in uni's Parquet reader (25% of engine CPU: `RandomAccessFileSource.read` byte arrays, and `LongBuffer.reserve` growing value buffers). The build arrays (`valid`, `chain`, `heads`) are only ~3%, so skipping the identity `valid` array would save little. A pre-touched heap (`-Xms24g -XX:+AlwaysPreTouch`) was not faster.
  - q7: the probe and Parquet decoding, spread out (no single hot spot).

## Design

Lane: Standard (a new logical rewrite, measured at SF10).

- New pass `GroupKeySemiJoins` (own file in `runtime/`), toggle `group_key_semi_joins` (on by default), run after `ReorderJoins` and before `PruneColumns` / `PushScanFilters`, so the copy below is pruned and gets its row-group filters.
- It matches a hash join `j` whose one side `B` is, through projections and deterministic filters, an `Aggregate` with group keys, and whose key pair `(o, b)` has `b` a pass-through of a group key `g`. `B`'s rows that match no row of the other side `O` never reach the output when `j` is:
  - Inner, Left, or Semi, with B on the right. B stays on the right so that O, lowered first, takes the runtime join filters of the scan column ids the copy of S shares with it (the B-on-the-left case fired on no TPC-H / TPC-DS plan).
  - Not Right / Full (unmatched B rows stay), and not Anti (a NOT IN reads NULL keys of B).
- A key source for `o` in `O`: a subplan `S` and its column `s` with every non-NULL value of `o` among the values of `s`. It is traced through filters, pass-through projections, either side of an inner join (and across its key equalities to the other side), the left side of a semi join (and its keys to the right), and the preserved side of a left join. The smallest candidate by estimated rows (filtered by `Stats.selectivity`) wins. It has to be a scan under filters and projections, or a shared CTE read, so that reading it again is cheap.
- Rewrite: the aggregate's input `I` becomes `I ⋉ π_s(S)` on `g = s`. Filtering by a group key commutes with grouping, so the groups with `g` in `S` keep all their rows, and the others are dropped before they are computed.
- Only when it pays: estimated rows of `S` at most 1/8 of the group key's estimated distinct values (not all groups: q34's per-ticket-and-customer counts would otherwise semi-join with all of `customer`), and `S`'s scan at most the aggregate's input rows. The semi join builds on the small `S` and gives `I`'s scan a runtime range filter as usual.
- The copy of `S` keeps its column ids: the semi join outputs only `I`'s columns, so no node sees both copies (`PlanValidator` checks ids per node). The rewrite skips an aggregate whose input is already such a semi join, so a re-plan doesn't add a second one.
- Lean: the pass is not in the listed areas of `design/formal-verification.md` (it is separate from `Planner.decorrelateScalar`), so no model; a unit test covers each join kind it accepts and refuses, with NULL keys.
- Tests: `EngineGroupKeySemiJoinsTest` (rewrite and refusals, NULL keys on both sides, results match with the toggle off), the toggle in `OptimizerOptionsTest`, `EngineOptimizerToggleTest`, and regenerated golden plans for the queries whose plans change.
- Measure with `WVLET_ENGINE_BENCH_AB=group_key_semi_joins`: SF10 TPC-H and TPC-DS (RUNS=3), SF1 both suites (RUNS=5), and changed queries again with RUNS=10 next to unchanged controls.

## Results

`WVLET_ENGINE_BENCH_AB=group_key_semi_joins` (on / off, one JVM, load average 15–20):

| suite | runs | wall | CPU | geomean |
|---|---:|---:|---:|---:|
| TPC-H SF10 | 3 | 0.923 | 0.925 | 0.944 |
| TPC-DS SF10 | 3 | 0.992 | 0.988 | 1.001 |
| TPC-H SF1 | 5 | 0.969 | 0.941 | 0.967 |
| TPC-DS SF1 | 5 | 1.015 | 1.020 | 1.009 |

- Plans change for TPC-H q2 and q17, and TPC-DS q32, q64, and q92. The queries on the "most regressed" lists all have unchanged plans (noise).
- Rechecked with RUNS=10 at SF10: TPC-H q17 0.50 (312 vs 626 ms), q2 1.00; TPC-DS q32 0.88, q64 0.93, q92 0.98; controls TPC-H q1 0.93, q3 0.96, TPC-DS q01 0.88, q03 0.97, q17 0.98.
- q17 at SF10 is now about 2–2.8x DuckDB (it was 5.6–7x). q20 is not reduced: `p_name like 'forest%'` gets the default selectivity, which misses the 1/8 bar.

## Alternatives and Why Not?

- **Push the outer keys in while decorrelating (`Planner.decorrelateScalar`)**: covers only scalar subqueries and changes a Lean-modeled function; the pass also covers joins to grouped CTEs and views.
- **A set-membership runtime filter (bloom / bitmap) from a build to the aggregate's scan**: needs the probe-side build to finish before the aggregate's input is read, which reorders the builds of one pipeline (#2362); a larger executor change.
- **Skip the identity `valid` array in q21's builds**: ~3% of q21's CPU per the profile.
