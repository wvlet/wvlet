# Engine: concurrent shared CTEs and parallel pipelines above breakers

## Goals

- Keep more of the 18 cores busy on the worst TPC-DS SF1 queries. Today the engine averages only 3.3–7.3 busy cores, and the calling thread is busy for 48–80% of each query's wall time running real operators on one thread.
- Close the largest remaining gap to DuckDB (q02 7.3x, q97 4.1x, q95 3.9x, q04, q78, q14, q11, q23) without changing any result row or row order.
- Report busy cores per query (CPU time / wall time) next to the A/B timings, so the gain is visible apart from noise on the shared machine.

## Background

- Morsels (adr/2026-11-22-engine-morsel-parallelism.md): `Parallelizer` turns a splittable scan plus the Filter / Project / hash-join probes above it into a `MorselOp`, which runs one copy per part on the fixed `TaskRunner` pool. Everything above a pipeline pulls on the calling thread.
- Invariant: **tasks never wait on tasks**. Join builds and other shared inputs are prepared on the calling thread before parts start, so the fixed pool can't deadlock. Any new concurrency must keep it.
- Shared CTE results (`SharedResult.batches`, a lazy val that calls `op.drain()`) are computed on whichever thread reads them first: the calling thread. At plan time, `replanCTEBodies` and then `replanJoinedCTEs` materialize joined CTEs one by one (`joined.foreach(_.materialize())`); the rest are computed lazily on first read during execution.
- `finalizeCTEs` finalizes consumers before the CTEs they read (`finalizeOrder`), so producers come last in that order.
- Pool tasks never read a CTE result: scan splits and join builds are taken in `MorselOp.next` on the calling thread before parts are submitted. That is about 70% of calling-thread time in q02 and q04 and 33% in q95.
- A CTE result is already a splittable source (`CTETable` / `SharedTable` split by batch), so what runs above a CTE reader is parallel. The serial part is the CTE *body*'s top: the operators above its breakers.
- Breakers (`PartitionedAggOp`, `HashAggOp`, sorts, windows) parallelize their own work internally, but their output is not a splittable source. So the probes, filters and projections above an aggregate run on the calling thread: in q78, 72% of calling-thread time is the left joins over three `PartitionedAgg` outputs, under the root sort.
- Scala.js has no threads (`TaskRunner.Sequential`); the same plans must run there unchanged.

## Design

Lane: Standard — a change to the engine's execution model, split into several PRs, each behind an `Optimization` toggle.

### PR 1: Compute independent shared CTEs concurrently

- Add a *coordinator* runner to `TaskRunnerCompat`: plain threads (not pool threads) on JVM and Native, inline on Scala.js. A coordinator drains one CTE result and submits that body's morsels to the shared pool. Only coordinators and the calling thread ever block, never pool tasks, so the invariant holds.
- Where CTEs are materialized together (`replanCTEBodies`, `replanJoinedCTEs`), and when the root plan starts executing (every finalized CTE the plan reads and nothing has computed yet), start them at once on coordinators.
- Every CTE is finalized before any coordinator starts. Coordinators touch only the result's batches; row counts and other planner state are set on the calling thread after the join.
- Start producers first (`finalizeOrder` reversed). A consumer blocks on its producer's result on its own coordinator. A coordinator that needs a producer no one has started computes it inline, so a cap on coordinators can't deadlock.
- An eager computation keeps its batches or its error, and an error is raised only when a reader reads that result. So a CTE that today is never read (for example, a reader under a filled LIMIT) can't fail a query that succeeds today. Errors that happen concurrently are raised in a fixed CTE order, so the reported error stays deterministic.
- The query owns its coordinators: closing the root, or an error, cancels them and waits until they stop, so no work outlives the query.
- Every CTE still computes exactly once, and readers read the same batches in the same order, so results and row order are unchanged.
- Cap the number of coordinators running at once (about parallelism / 2), so a query with dozens of CTEs doesn't hold every result in memory at once.
- Parallelism 1 computes them in order on the calling thread, as today.
- Toggle: `concurrent_shared_ctes`.

### PR 2: Breaker outputs as splittable sources

- Wrap a breaker's output (`PartitionedAggOp` first, then `HashAggOp` above a size threshold) as a source whose parts are its output batches, the same way a CTE result already is. `Parallelizer` then builds a `MorselOp` over it, so the joins, filters, projections and partial aggregations above an aggregate run on the pool.
- The breaker is drained in `MorselOp`'s prepare step, on the calling thread, before parts are submitted (the breaker itself already uses the pool). This keeps the invariant.
- `PartitionedAggOp` emits one batch run per key partition; parts stay in partition order, so output equals the sequential plan.
- Applies inside CTE bodies too, so with PR 1 the bodies' tops run on the pool.
- Also applies on hash join build sides. `Parallelizer` already rewrites a build input, so a probe pipeline over an aggregate that feeds a build (the q78 shape) becomes a `MorselOp`, and the build is then made from its output as today.
- Toggle: `parallel_above_breakers`.

### PR 3 (if the profile still shows it): other breakers

- Same wrapping for other breakers that show up in the re-profile (e.g. the final phase of two-phase aggregation in q97 / q02, a large sort or window feeding a join). Decided from the profile after PR 2, not up front.

### Validation (each PR)

- Amend adr/2026-11-22-engine-morsel-parallelism.md: coordinators (PR 1), and pipelines above breakers, replacing "operators above a pipeline pull on the calling thread" (PR 2).
- TPC-H 22/22, TPC-DS 99/99, spec/basic conformance, golden plans, engineJS / engineNative compile; the new toggles listed in `OptimizerOptionsTest` and covered by `EngineOptimizerToggleTest`.
- Parallel tests lower thresholds so small data takes the concurrent paths (as `EngineTPCHParallelTest` does).
- SF1 A/B through the toggle on one build, alternating rounds, with unchanged-plan queries as the noise control; busy cores per query before and after.

## Alternatives and Why Not?

- **Run CTE bodies as pool tasks.** A body contains `MorselOp`s that submit to the same pool and wait, so pool threads would wait on pool tasks and the fixed pool can deadlock.
- **A push-based (pipeline-breaker-aware) scheduler for the whole plan.** The right long-term model, but a rewrite of every operator; morsel sources over breakers get most of the gain inside today's pull model.
- **Fuse the pipeline above an aggregate into the aggregate's partition tasks.** Saves materializing the aggregate's output, but couples every breaker to the Parallelizer's pipeline rules; worth it later only if the materialization shows up in the profile.
