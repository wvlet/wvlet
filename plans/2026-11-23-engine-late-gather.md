# Engine: late gather (selection vectors) for Filter and hash join probes

## Goals

- Stop copying every column of every batch in `FilterOp` and `HashJoinOp.probeBatch`; that is 21% of engine CPU on TPC-DS q78 and q04 (SF1), and 11% on q49, plus GC pressure.
- Gather each column once, when an operator first reads it, instead of once per Filter or join it passes through.
- No change in results; keep the representation plain arrays for Scala.js and Scala Native.

## Background

- `Batch` is a dense `Array[Vec]`. `FilterOp` turns its mask into row indices and gathers every kept column; a join probe gathers every probe and build column for the matched pairs, and again for the residual condition.
- A chain like scan → filter → join → join → aggregate copies a column at every step, even though only the last reader needs it, and a column used by no later operator is copied anyway.
- `Vec.gather` already treats index -1 as NULL (outer-join padding). Composing index arrays must keep -1 as -1.
- Expressions read columns through `ColRef.eval` (`b.columns(index)`); other operators mostly read `b.columns` as a whole.
- No exec operator reads `OptimizerOptions` today: planner passes check toggles.

## Design

Lane: **Express**. An internal engine refactor with no change in behaviour, behind an `Optimization` toggle, touching no always-gate area. Per the launch prompt, the PR is not merged.

- A `Batch` column can be *deferred*: a source `Vec` plus a row-index array, shared by every column that came through the same gather. `column(i)` gathers on first read and caches the result; `columns` materializes all of them, so existing callers keep working unchanged.
- A new lazy `Batch.take(idx, n)` defers; `gather` stays eager. A `take` over deferred columns composes the index arrays (a -1 stays -1), so a chain of Filters and joins ends in one gather per column, done by its reader. `select` (column picks) and `numColumns` keep columns deferred; `slice`, `row`, and `columns` materialize.
- `FilterOp` and `HashJoinOp` (output, residual check, unmatched-build rows) call `take`. `ColRef` reads through `column(i)`, so projections, aggregates, and join keys gather only the columns they reference.
- Deferred batches never leave the operator pipeline that made them. `drain()` and `Batch.concat`, including the single-batch case, compact them (gather the rest and drop the sources). That covers materialized builds, CTE results, morsel parts, and partitioned aggregation, so a deferred batch is never shared across threads or kept with its whole source batch. This makes the column cache single-threaded, which also suits Scala Native.
- Other operators migrate in later PRs.
- New toggle `late_gather` (`Optimization.LateGather`). The planner sets it on `FilterOp` and `HashJoinOp` after column pruning, and the Parallelizer's copies keep it. When it is disabled, both gather eagerly as today. It is listed in `OptimizerOptionsTest` and covered by `EngineOptimizerToggleTest`.
- Tests cover deferred batches (composition, -1 padding through a deferred build in LEFT/FULL joins, caching, compaction), plus the TPC-H, TPC-DS, and spec/basic conformance gates. The PR includes an SF1 A/B via the toggle.

## Alternatives and Why Not?

- A `Vec` subclass that wraps a selection: every kernel's `match` on `LongVec`/`IntVec`/... would miss it or need a case; too wide for one PR.
- A selection vector for the whole batch: join output mixes probe and build columns with different index arrays, so selections must be per column (shared by group).
- Gathering only the downstream columns at plan time (as Filter `keep=` does): already done for Filter; it does not remove the repeated copies along a chain.
