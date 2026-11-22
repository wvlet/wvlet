# ADR: Morsel-driven parallelism in the columnar engine

Date: 2026-11-22 · PR #2210

## Context

After column pruning and faster kernels, `wvlet-engine` ran TPC-H sf=1 in 8.6 s on one thread, against 0.33 s for DuckDB on all cores. The profile showed most time spent decoding Parquet row groups, evaluating filters and projections, probing hash joins, and aggregating. All of these are independent per row group. The engine is pull-based (`Operator.next()`) and cross-platform: Scala.js has no threads.

## Decision

- **Morsels are a source's own parts.** `TableSource.scanSplits` cuts a scan into independent parts. For a single Parquet file these are the row groups that row-group pruning keeps (`selectRowGroups`, then `readRowGroup` per part). Datasets and other sources don't split yet.
- **Plans keep their shape.** A post-planning pass (`Parallelizer`, run after `ColumnPruner`) finds *pipelines*: a splittable scan with the Filter / Project / hash-join probe operators above it. It replaces each pipeline with a `MorselOp`. A `MorselOp` rebuilds a fresh copy of the pipeline over each part and runs the copies as tasks. There is no push-based rewrite of the engine: operators above a pipeline still pull on the calling thread.
- **Output stays deterministic.** `MorselOp` keeps up to twice the parallelism of parts in flight and emits results in part order, so the output equals the sequential output (LIMIT without ORDER BY and spec outputs stay stable).
- **Hash joins share one build.** Probe-side splitting applies to joins whose output for a probe row depends only on that row: inner, left, semi, anti and cross. The build (`JoinBuild`) is made once, before any part starts, *on the calling thread*. It is then read-only and shared by the copies. Right and full joins split too. Their copies mark matched build rows in an array the build shares, which they only ever set to true. Once every part has finished, a *tail* copy of the pipeline runs over an empty input on the calling thread and emits the unmatched build rows, through whatever operators sit above the join (PR #2214).
- **Aggregation runs in two phases** when every aggregate's partial results combine with another aggregate:
  - count is combined by sum; sum, min and max by themselves; avg as a sum and a count
  - each part aggregates on its own, a final `HashAggOp` combines the partial rows by key, and a projection restores the original types
  - other aggregates (DISTINCT, quantiles, string_agg, ...) keep the sequential plan
- **Hash tables partition by key.** A large join build (at least 64K rows) is split by the hash of its key into 2× parallelism partitions. Each partition's table is built as its own task, and probes route rows to their partition. Grouped aggregations, the final phase of two-phase aggregation included, partition their input the same way, so groups never span partitions and any aggregate works (DISTINCT, quantiles, ...). The partition comes from the hash's top bits, which a partition's own table never uses as slot bits. Below 64K rows, aggregation stays on one thread in first-seen group order (PR #2218).
- **Semi / anti joins probe the larger side.** When an EXISTS / IN subquery is estimated at over 4x the outer input, the hash table holds the outer rows instead (RIGHT SEMI / RIGHT ANTI). The subquery's rows are probed in parallel parts, which mark the matches, and the tail emits the matched (or unmatched) outer rows once.
- **Tasks never wait on tasks.** Everything a part needs from other operators (join builds) is prepared before parts are submitted. So a fixed thread pool can't deadlock, even when a build input is itself a `MorselOp`.
- **The runner is platform code.** `TaskRunner` is a fixed daemon pool on JVM and Scala Native (`TaskRunnerCompatImpl`), sized by `WVLET_ENGINE_PARALLELISM` / `wvlet.engine.parallelism` or the processor count. Scala.js runs tasks sequentially as they are submitted, so the same plans run unchanged.

## Consequences

- TPC-H sf=1 dropped from 8.6 s to 2.1 s (6.4x DuckDB's total, from 26x). See `plans/2026-11-21-columnar-engine.md`.
- Expressions must be safe to share across threads. They are immutable apart from benign caches: `Const`'s last vector, and the synchronized regex cache. New expressions must keep it that way.
- Parallelism is per pipeline, not across the whole plan. A sort, a final aggregation, or a window still runs on one thread above its pipelines.
- Outer hash joins build on the smaller input. When the build side is estimated to be over 4x the probe side, LEFT becomes RIGHT (and back) with the inputs swapped, so the large input is probed in parallel. TPC-H q13 went from 395 ms to 62 ms this way.
- `EngineTPCHParallelTest` rewrites the TPC-H data in 2048-row row groups, so that every query exercises the parallel paths against DuckDB.
