# ADR: Filters and hash joins defer their output columns until a reader needs them

Date: 2026-11-23 · PR #2319

## Context

`FilterOp` and `HashJoinOp.probeBatch` gathered every column of every batch (`Batch.gather`), so a scan → filter → join → join chain copied each column once per operator, including columns that no later operator reads. On TPC-DS SF1 this was 21% of engine CPU on q78 and q04 and 11% on q49, plus GC pressure. Join output mixes probe and build rows, so a single selection vector per batch cannot describe it.

## Decision

- A `Batch` column can be deferred: a source `Vec` plus a row-index array (-1 for a row of NULLs), shared by the columns that came through the same `take`. `column(i)` gathers on first read and caches the result; `columns` and `compact()` gather all of them, so callers that need dense vectors are unchanged.
- `take` over deferred columns composes the index arrays instead of gathering, so a chain of filters and joins ends in one gather per column, done by its reader. `gather` stays eager.
- `FilterOp` and `HashJoinOp` emit deferred batches; `ColRef` reads through `column(i)`. Other operators keep gathering eagerly until they are migrated.
- Deferred batches never leave the pipeline that made them: `Operator.drain()` and `Batch.concat` (including the single-batch case) compact. Batches that are buffered or shared across threads (join builds, CTE results, morsel parts, partitioned aggregation) are therefore dense, the column cache is touched by one thread only (also on Scala Native), and no buffer keeps a whole source batch alive for a few selected rows.
- `late_gather` (`Optimization.LateGather`) turns this off. Exec operators do not see `OptimizerOptions`, so the planner sets a flag on the filters and joins after column pruning, Parallelizer copies keep it, and EXPLAIN marks eager operators `eager`.

## Consequences

- Each `Vec.gather` binds the gathered nulls to a local before building the result vector. An inlined loop passed directly as a constructor argument runs with a non-empty operand stack, which HotSpot cannot enter by on-stack replacement. After an uncommon trap, a long `StrVec.gather` then stayed in the interpreter (TPC-DS q43: 81 ms vs 28 ms after q01–q42).
- A consumer that calls `columns` on a deferred batch pays the old full gather plus one index composition per distinct selection.
- Code that buffers batches outside `drain()` must call `compact()` first.
