# Engine memory accounting (query manager phase 2)

## Goals

- Concurrent queries on `wvlet-server` can exhaust the heap today: the in-process engine has no per-query or server-wide memory budget ([query manager plan](2026-11-27-query-manager.md), phase 2).
- A query that grows past its own limit fails fast with `RESOURCE_EXHAUSTED` instead of taking the server down with an `OutOfMemoryError`.
- When the server as a whole is over its memory pool, `QueryService` holds queued queries until running ones release memory.
- Local CLI and REPL runs behave as today: no limits unless configured.

## Background

- The heap is dominated by a few holders that keep their rows until the query ends: join build tables (`JoinBuild`), hash aggregation tables (`HashAggOp`, `PartitionedAggOp`), sort and window buffers (`SortOp`, `WindowOp`), shared CTE results, and the drained result batches.
- Operators are built by a per-query `Planner` in many places (14 `HashAggOp` sites, `Parallelizer` rebuilding joins), and some plan-time subqueries run while planning. Threading a context through every constructor would touch most of the planner.
- Builds run on `TaskRunner` worker threads. Phase 3 (a parallel session) changes `TaskRunner` scheduling, so this work must not touch it.
- The server gives each session its own `WvletScriptRunner` and engine, so a server-wide budget has to be an object the server shares across engines.

## Design

Lane: Standard — new server behaviour and options, and a new engine failure mode.

- **Pool and context.** A server-wide `MemoryPool` (an atomic byte counter with a soft capacity) and a per-query `MemoryContext` (bytes reserved by one query, an optional per-query limit). Pure Scala in `wvlet-engine`, shared across JVM, JS, and Native.
- **Reserve in coarse chunks while a holder grows, release when the query ends.** Holders reserve as input batches arrive and as tables grow (e.g. per 1 MB of estimated bytes), so the per-query limit fires before the large allocation, not after it. Sizes are estimated from vectors: primitive arrays by width, strings from a small sample of lengths.
- **One context per `WvletEngine.execute` call.** It opens before planning (plan-time subqueries and CTEs drain while planning) and closes in one outer `finally` that returns all its bytes to the pool and clears the thread's ambient context, so a reused server worker thread never carries it into the next query. A statement that runs several engine queries (`save`, `append`, `delete`) gets a limit per engine query.
- **Buffered results** count against the query's limit while `execute` drains them; once returned, the caller owns them (the server's retained rows stay bounded by its existing row limits).
- **How operators find their context.** The planner sets the query's context as the ambient context of the planning thread while it builds operators; memory-holding operators capture it in a field at construction. Operators that build child operators at run time on worker threads (`PartitionedAggOp`'s per-partition `HashAggOp`s) pass their own captured context to them. An operator built outside any scope (tests, tools) gets a no-op context.
- **Over the per-query limit:** the reserving call throws `RESOURCE_EXHAUSTED` ("query exceeded its memory limit of N MB"), which fails the query through the normal error path; the context still releases on close. Plan-time fallbacks that catch `NonFatal` (`InSubqueryAggregates`, `SharedScans`) rethrow it instead of planning around it.
- **Over the pool:** reservations still succeed (no spill, no killing in this phase); the pool reports pressure. `QueryService`'s dispatcher starts no queued query while the pool is over capacity, and re-checks when a query completes. Failing the largest query under sustained pressure (Trino's low-memory killer) stays a follow-up, recorded when phase 2 is ticked.
- **Toggle.** `Optimization.MemoryAccounting` (on by default), listed in `OptimizerOptionsTest` with a `noPlanChange` entry in `EngineOptimizerToggleTest`; off means operators get the no-op context. A/B with `WVLET_ENGINE_BENCH_AB=memory_accounting` under the bench lock, numbers in the PR.
- **Server options.** `--max-query-memory <size>` (per-query limit, default none) and `--memory-pool-size <size>` (default 70% of max heap), with sizes like `512MB` / `4GB`; they go into `QueryManagerConfig`, and the shared pool reaches each session's engine through `QueryExecutor` and the engine connector. The CLI and REPL keep an unbounded pool.
- **Landing in three PRs:** 1) pool, context, toggle, join builds, buffered results; 2) aggregation, sort, window, CTE results; 3) server admission and options plus user docs. `design/engine.md` and `design/server.md` (Limits) are updated with each.
- Tests cover a query over a tiny limit failing with `RESOURCE_EXHAUSTED` and releasing its bytes, the pool returning to zero after normal queries (also on engineJS), and the server holding queued queries while the pool is over capacity.

## Alternatives and Why Not?

- **Pass a `MemoryContext` to every operator constructor.** Explicit, but touches dozens of planner sites and every `Parallelizer` rebuild for no behavioural gain.
- **Bind contexts by walking the finished operator tree.** Misses operators that run during planning (scalar subqueries, plan-time CTEs).
- **Reserve per batch as rows arrive (Trino style).** Accurate peaks, but adds bookkeeping to hot loops; per-holder reservations cover the large allocations at near-zero cost.
- **Measure real heap use (`Runtime.totalMemory`) for admission.** Not per-query, lags GC, and is unavailable in the same form on JS and Native.
