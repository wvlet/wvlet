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

- `QueryManagerConfig(maxRunningQueries, maxQueuedQueries, finishedQueryRetention, maxFinishedQueries)` with `withXXX` methods; defaults: running = available processors / 2 (at least 1), queued = 1000, retention = 1 hour.
- Admission: a fixed worker pool of `maxRunningQueries` threads fed by a bounded queue. `enqueue` past `maxQueuedQueries` throws `StatusCode.RESOURCE_EXHAUSTED`, so clients retry. Queries stay QUEUED until a worker takes them; a query cancelled while queued never starts (as today).
- Per-session ordering stays: a worker taking a query whose session is busy must not block the pool. Keep a per-session FIFO and dispatch only the head of each idle session, instead of `runner.synchronized` inside the worker.
- Events: `QueryEventListener` with `queryCreated(QueryCreatedEvent)`, `queryStarted`, `queryCompleted(QueryCompletedEvent)`. A completed event carries query id, session id, profile, status, status code, error message, created/started/completed times, queued and run wall time, and result rows. Listener failures are logged and never fail a query.
- Default listener: a JSON-lines file under the server's work folder (e.g. `.wvlet/logs/query-events.jsonl`), rotated daily; plus a debug log line per event.
- Retention: evict finished queries older than the retention or beyond `maxFinishedQueries` (oldest first) on each enqueue; drop result rows of evicted queries. `fetchNext` of an evicted id returns INVALID_ARGUMENT as for an unknown id.
- Ids: keep ULIDs. They sort by creation time and already round-trip through the API ([`UlidRpcRoundTripTest`](../wvlet-server/src/test/scala/wvlet/lang/server/UlidRpcRoundTripTest.scala)).
- Applies to every engine the runner targets (Wvlet engine, DuckDB, Trino): admission does not depend on the engine.
- Expose current counts (running, queued, finished in memory) through a small status API for the UI and tests.

### Phase 2: engine memory accounting (wvlet-engine)

- A per-query `MemoryContext` that reserves bytes from a server-wide `MemoryPool`; child contexts per operator.
- Instrument only the large holders: join build tables, hash aggregation tables, sort and window buffers, CTE materializations, buffered results.
- Over a per-query limit: fail the query with `RESOURCE_EXHAUSTED`. Over the pool: the query manager stops admitting; if reservations still block, fail the largest query (Trino's low-memory killer). Spill is out of scope.
- Behind an `Optimization` toggle per [engine.md](../design/engine.md); bench to confirm reservation bookkeeping costs nothing measurable.

### Phase 3: fair CPU sharing (wvlet-engine)

- Give `ThreadPoolRunner` a task queue per query and pick across queries round-robin, or by a multilevel feedback queue on accumulated task time, instead of one FIFO.
- Measure with a mixed workload: one long TPC-DS query plus a stream of short queries; the target is short-query latency close to running alone.

### Phase 4 (only when needed): resource groups

- A tree of groups with their own running/queued limits and selectors by user, source, or tag, once multiple users share a server.

## Tests

- Admission: with `maxRunningQueries = 1`, a second query stays QUEUED until the first finishes; past `maxQueuedQueries`, enqueue fails with RESOURCE_EXHAUSTED.
- Session ordering: two statements of one session run in order, while a query of another session runs in parallel, with one worker blocked on neither.
- Cancel: queued and running cancellation still end CANCELED and free the slot.
- Events: an in-memory listener sees created → started → completed for success, failure, and cancel, in order, with consistent times.
- Retention: finished queries past the retention are evicted, unfinished ones never.

## Alternatives and Why Not?

- Trino-style resource groups first: a flat group covers single-user and small-team servers; groups add config and selectors before anyone needs them.
- Queue in the engine's `TaskRunner` only: it controls CPU, not queries routed to DuckDB or Trino, and gives no query-level events.
- Trino's query id format (`yyyyMMdd_HHmmss_seq_node`): ULIDs already sort by time and are in the API.
