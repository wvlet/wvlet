# Query manager for the Wvlet server

## Goals

- Make the server's query queue real: bound concurrent and queued queries, and reject past the queue limit.
- Log query lifecycle events (created, started, completed) with stats, through a listener interface.
- Keep issuing one query id per request (ULID, already in place) and bound how long finished queries stay in memory.
- Later phases: per-query memory accounting in the engine, and fair CPU sharing across concurrent queries.

## Current state

- [`QueryService`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryService.scala) issues a ULID per query, tracks QUEUED → RUNNING → FINISHED / FAILED / CANCELED, and cancels by interrupting the worker. Statements of one session are serialized on its runner; sessions run in parallel.
- Queries run on an unbounded cached thread pool, so QUEUED is only a label: every query starts at once.
- `queryMap` keeps every query and its result rows forever.
- The engine runs all queries' tasks on one fixed pool with a FIFO queue ([`TaskRunnerCompatImpl`](../wvlet-engine/.jvm/src/main/scala/wvlet/lang/engine/exec/TaskRunnerCompatImpl.scala)): a large query's morsels delay short queries.
- The engine has no per-query or server-wide memory budget.

## How other systems split the problem

| Layer | Trino | DuckDB | Others |
| --- | --- | --- | --- |
| Admission | Resource groups: tree of groups with `hardConcurrencyLimit`, `maxQueued`, selectors by user/source/tags, fair/weighted/priority policies | None; every connection runs at once | Snowflake `MAX_CONCURRENCY_LEVEL` + queue timeout; Redshift WLM queues; ClickHouse `max_concurrent_queries` and workloads; Postgres relies on pgbouncer |
| Memory | Memory-context tree (query → task → operator) reserving from a pool; `query.max-memory`(-per-node); low-memory killer; spill | One global `memory_limit` in the buffer manager; evict and spill | ClickHouse per-query `max_memory_usage` + server limit; Postgres `work_mem` per operator |
| CPU | Time-sharing executor, ~1 s quanta, multilevel feedback queue (long queries lose priority) | One task scheduler shared by connections | Spark FAIR scheduler pools |
| Ids and events | `QueryStateMachine`; `EventListener` SPI (`queryCreated`, `queryCompleted`, `splitCompleted`) | `QueryLog` logging | ClickHouse `system.query_log` (QueryStart / QueryFinish); Snowflake `QUERY_HISTORY` |

DuckDB's 2026 [Quack](https://duckdb.org/2026/05/12/quack-remote-protocol) client-server protocol adds multi-process writers but documents no admission or per-query limits; Trino is the closer model.

## Design

### Phase 1: query manager (wvlet-server)

- `QueryManagerConfig(maxRunningQueries, maxQueuedQueries, finishedQueryRetention, maxFinishedQueries)` with `withXXX` methods; defaults: running = available processors / 2 (at least 1), queued = 1000, retention = 1 hour, 1000 finished queries. `wvlet ui --max-running-queries n --max-queued-queries n` sets the first two.
- Admission: a fixed worker pool of `maxRunningQueries` threads fed by a bounded queue. `enqueue` past `maxQueuedQueries` throws `StatusCode.RESOURCE_EXHAUSTED`, so clients retry. Queries stay QUEUED until a worker takes them; a query cancelled while queued never starts (as today).
- Per-session ordering stays: a worker taking a query whose session is busy must not block the pool. Keep a per-session FIFO and dispatch only the head of each idle session, instead of `runner.synchronized` inside the worker.
- Events: `QueryEventListener.onEvent(QueryEvent)`, one event type with `eventType` CREATED / STARTED / COMPLETED, so a log line has one shape. A completed event carries query id, session id, profile, status, status code, error message, created/started/completed times, queued and run wall time, and result rows. Listener failures are logged and never fail a query.
- Default listener: JSON lines in `<target>/logs/query-events-yyyy-MM-dd.jsonl` (next to `wvlet-out.log`), one file per UTC day; plus a debug log line per event.
- Retention: evict finished queries older than the retention or beyond `maxFinishedQueries` (oldest first) on each enqueue; drop result rows of evicted queries. `fetchNext` of an evicted id returns INVALID_ARGUMENT as for an unknown id.
- Ids: replace ULIDs with Trino-style `yyyyMMdd_HHmmss_<seq>_<server>`, e.g. `20261127_155437_00042_x7k2p`. Two ULIDs (or UUIDv7s) issued close together share their leading characters and differ only in the trailing random part, so people cannot tell queries apart at a glance.
  - Time: the query's creation time in UTC, to the second, so ids sort by time and show when the query ran.
  - Sequence: a 5-digit counter per server process (wrapping at 99999) that changes at a fixed position between neighboring queries.
  - Server: 5 random lowercase base-32 characters chosen at server start, so ids from multiple servers, or from one server across restarts, never collide without a persisted counter.
  - `QueryInfo.queryId`, `QueryResponse`, `QueryInfoRequest`, and `QueryCancelRequest` carry it as `String`. A `QueryId` value class was dropped: uni's router encodes return values with runtime weavers that ignore custom givens, so it would go over the wire as an object. `QueryIdGenerator.isQueryId` checks the shape; `QueryIdRpcRoundTripTest` replaces `UlidRpcRoundTripTest`.
  - Clients may pass their own id through `QueryRequest.requestId` for idempotent retries: a repeated request id returns the existing query instead of starting a new one.
- Applies to every engine the runner targets (Wvlet engine, DuckDB, Trino): admission does not depend on the engine.
- `FrontendApi.status` reports `runningQueries` and `queuedQueries`.

### Phase 2: engine memory accounting (wvlet-engine) — done

- [x] Landed in #2573 (pool, context, toggle, join builds, buffered results), #2574 (aggregation, sort, window, shared CTE results), and #2575 (server pool, admission, `--max-query-memory` / `--memory-pool-size`); see the [design note](2026-11-27-engine-memory-accounting.md).
- Remaining follow-ups: the low-memory killer (fail the largest query when the pool stays over capacity), releasing reservations before the query ends (no per-operator child contexts yet), and counting `DistinctOp`, set operations, and nested-loop builds.

- A per-query `MemoryContext` that reserves bytes from a server-wide `MemoryPool`; child contexts per operator.
- Instrument only the large holders: join build tables, hash aggregation tables, sort and window buffers, CTE materializations, buffered results.
- Over a per-query limit: fail the query with `RESOURCE_EXHAUSTED`. Over the pool: the query manager stops admitting; if reservations still block, fail the largest query (Trino's low-memory killer). Spill is out of scope.
- Behind an `Optimization` toggle per [engine.md](../design/engine.md); bench to confirm reservation bookkeeping costs nothing measurable.

### Phase 3: fair CPU sharing (wvlet-engine) — done

- Give `ThreadPoolRunner` a task queue per query and pick across queries round-robin, or by a multilevel feedback queue on accumulated task time, instead of one FIFO.
- Measure with a mixed workload: one long TPC-DS query plus a stream of short queries; the target is short-query latency close to running alone.
- Done: per-query `TaskGroup`s and a multilevel feedback queue counting running tasks, behind `fair_task_scheduling` ([plan](2026-11-27-fair-task-scheduling.md), [engine](../design/engine.md#fair-task-scheduling)). No TPC-H/TPC-DS query runs long enough at SF10, so `EngineMixedWorkloadBench` uses a lineitem group-by as the long query. Short-query p90 dropped by about 1.5x next to it; the median still ran 1.5–4.8x the alone time, from non-preemptible long-query morsels and GC pauses.

### Phase 4 (only when needed): resource groups

- A tree of groups with their own running/queued limits and selectors by user, source, or tag, once multiple users share a server.

## Tests

- Admission: with `maxRunningQueries = 1`, a second query stays QUEUED until the first finishes; past `maxQueuedQueries`, enqueue fails with RESOURCE_EXHAUSTED.
- Session ordering: two statements of one session run in order, while a query of another session runs in parallel, with one worker blocked on neither.
- Cancel: queued and running cancellation still end CANCELED and free the slot.
- Events: an in-memory listener sees created → started → completed for success, failure, and cancel, in order, with consistent times.
- Retention: finished queries past the retention are evicted, unfinished ones never.
- Query ids: format and parse round-trip, neighboring ids differ in the sequence, two server instances never issue the same id, and a repeated request id returns the existing query.

## Alternatives and Why Not?

- Trino-style resource groups first: a flat group covers single-user and small-team servers; groups add config and selectors before anyone needs them.
- Queue in the engine's `TaskRunner` only: it controls CPU, not queries routed to DuckDB or Trino, and gives no query-level events.
- Keep ULIDs or switch to UUIDv7: both put the timestamp first and the random part last, so neighboring queries look alike.
- Random UUIDv4 (ClickHouse, Athena): differs from the first character but does not sort by time and is long to quote.
- A persisted integer counter (Treasure Data jobs, Redshift): easiest to quote, but multiple servers need a shared counter service.
