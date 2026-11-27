# ADR: Decide a LEFT join's Bloom miss filter from its first probe batch, and size a prefiltered aggregation from the filter's sample

Date: 2026-11-26 · PR #TBD

## Context

- `probe_miss_filter` ([#2493](2026-11-26-engine-unmatched-left-probe.md)) gives a large flat join table (16 MB or more of entries) a Bloom filter of its keys, but only for joins whose probes are known at plan time to mostly miss: anti joins, unmatched-only LEFT joins, and the LEFT joins that a `left_match_prefilter` filter probes ([#2504](2026-11-26-engine-left-match-prefilter.md)). A plain LEFT join can't tell from its plan whether its probes mostly miss. A LEFT join from sales to a channel aggregate mostly misses, while a LEFT join to a dimension mostly matches, where the filter costs a check on every probe and saves nothing.
- The partial aggregations under a `left_match_prefilter` filter kept the group estimate of their unfiltered input. In TPC-DS q78 that is ~4.9M groups, capped per scan part at its rows (up to 2^18), while the filter keeps ~6K of ~5M rows. The first parts, which start before any part has reported its group count, zero-filled group tables and aggregate state for groups the filter drops.

## Decision

- `sampled_miss_filter`: the planner's `configureJoin` marks every LEFT join that doesn't already get the filter (`JoinBuild.sampledMissFilter`). After its flat table is built, a table of at least `LongJoinTable.MinMissFilterBytes` keeps its build keys and partition rows in a `MissFilterSample` (`exec/MissFilterSample.scala`).
- The first probe batch of at least 1024 rows decides. If at most half of its rows match, the thread that probed it builds a filtered copy of the table (`LongJoinTable.withMissFilter`: the same entries plus the Bloom filter that `build` makes), and later probes go through that copy. Either way the sample then drops the keys. The other threads keep probing the unfiltered table until the copy is published, and both tables give the same matches, so a race costs only speed.
- The filter is built after the sample, not with the table, so a join whose probes mostly match pays nothing at build time. It is built on one thread: the probe threads are runner tasks, and waiting on the runner from inside one of its tasks could stall it.
- `prefilter_presize`: a `HashAggOp` directly over a `FilterOp(BuildKeyMatch)` reads its first batch before it sizes its group table. The filter has then seen at least that batch, so the aggregation scales its group hint by the share of rows the filter has kept (`BuildKeyMatch.presize`). A filter that turned itself off, or that has seen no row yet (its builds weren't ready), leaves the hint as it is, so presizing is kept for the case #2504's ADR worried about.
- Both are executor-level changes with no plan-text change, so they are listed in `EngineOptimizerToggleTest.noPlanChange`.
- The new logic lives outside `Operators.scala` (`MissFilterSample`, `BuildKeyMatch`, `LongJoinTable.firstRows(Vec, ...)`). Adding even a 10-line helper method to `JoinBuild` made scalafmt rewrap ~120 unrelated lines of that file.

## Consequences

- Results: see the PR's SF10 A/B.
- The sample is one batch. A probe stream whose match rate changes after its first batch keeps the first decision.
- Only flat tables (a single integer key, or packed multi-column keys) can keep a filter. Hashed multi-key builds still have none.
- In the golden plans, the plain LEFT joins are in TPC-DS q06, q10, q35, q41, q72 and q77. The sales-to-returns outer joins of q75 and q80 are planned as RIGHT joins that probe the returns side, so this filter doesn't reach them.
