# Server

`wvlet-server` runs queries for the UI, the `wvlet` CLI's server client, and SDKs over the HTTP API. See the [system map](../ARCHITECTURE.md) for where it sits.

## Query lifecycle

- [`QueryService`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryService.scala) admits queries QUEUED → RUNNING → FINISHED / FAILED / CANCELED ([`QueryStatus`](../wvlet-api/src/main/scala/wvlet/lang/api/v1/query/QueryStatus.scala)). Clients poll `fetchNext` by id.
- Admission ([`QueryManagerConfig`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryManagerConfig.scala), `--max-running-queries` / `--max-queued-queries`): a fixed worker pool runs at most `maxRunningQueries`; past `maxQueuedQueries`, `enqueue` throws RESOURCE_EXHAUSTED. The limits apply to every engine, not only the in-process one.
- Per-session order: statements of one session run one at a time, in order, because a session's compiler is not thread-safe. The dispatcher starts the oldest queued query whose session is idle, so a busy session never holds a worker. [`ScriptRunnerSessions`](../wvlet-server/src/main/scala/wvlet/lang/server/ScriptRunnerSessions.scala) gives each session its own runner, so `use` changes only that session; idle sessions are evicted after an hour.
- Cancel is best effort: the status becomes CANCELED at once and later completion is discarded. A queued query leaves the queue and never starts; a running one keeps its slot until its statement returns.
- Query ids ([`QueryIdGenerator`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryIdGenerator.scala)) are `yyyyMMdd_HHmmss_<seq>_<server>` strings, see the [ADR](../adr/2026-11-27-trino-style-query-ids.md). They are `String` on the wire: uni's router encodes return values with runtime weavers that ignore a custom `given Weaver`, so a value class would not round-trip as a plain string.
- A resubmitted `QueryRequest.requestId` returns the query it started while that query is kept.
- Every query reports CREATED and COMPLETED [`QueryEvent`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryEvent.scala)s, plus STARTED unless cancelled while queued. The server logs them as JSON lines per UTC day under `<target>/logs`. Listener failures never fail a query.
- Finished queries (and their result rows) are dropped after `finishedQueryRetention` or beyond `maxFinishedQueries`; eviction runs on submission and completion.

## Limits

- CPU: in-process engine queries share one task pool, which picks tasks across queries by a multilevel feedback queue on their run time ([engine](engine.md#fair-task-scheduling)), so a short query isn't queued behind a long one's morsels. Coordinator tasks are picked across queries the same way, by their own run time, and a query computing shared CTE results runs those no coordinator has started itself. Tasks are not preempted, and queries routed to DuckDB or Trino are outside it.
- Memory: each in-process engine query counts its join builds, aggregation tables, sort and window buffers, shared CTE results, and buffered results against a `MemoryContext` ([engine](engine.md#memory-accounting)) and fails with RESOURCE_EXHAUSTED past `--max-query-memory` (`QueryManagerConfig.maxQueryMemory`, default none).
- At startup the server makes `QueryManagerConfig.newMemoryPool()` the process pool (`MemoryPool.setProcessPool`), so every session's engine shares it. `--memory-pool-size` (default 70% of the max heap) is a soft capacity: the dispatcher starts no queued query while the pool is over it and some query is running (only running queries release memory), and re-checks on every completion. No spill ([plan](../plans/2026-11-27-engine-memory-accounting.md)).
- Low-memory killer (`--low-memory-killer-delay`, `QueryManagerConfig.lowMemoryKillerDelay`, default 5 s): when the pool stays over capacity that long with more than one engine query open, `MemoryPool.killLargestIfStillOver` marks the query holding the most bytes; it fails with RESOURCE_EXHAUSTED through the normal error path (FAILED status, COMPLETED event). One victim at a time, and the delay restarts when the killed query closes. Reservations past capacity check it, and `QueryService` checks on a 500 ms timer, since a query holding memory without growing never reserves ([plan](../plans/2026-11-27-memory-follow-ups.md)).
- Memory counts only in-process engine queries; DuckDB and remote engines are not counted.

## Tests

- [`QueryManagerTest`](../wvlet-server/src/test/scala/wvlet/lang/server/QueryManagerTest.scala) holds queries in their slot with a listener that blocks STARTED events, so admission tests don't depend on query run times. With [`QueryServiceTest`](../wvlet-server/src/test/scala/wvlet/lang/server/QueryServiceTest.scala) and the RPC round trip: `./sbt "server/testOnly *QueryManagerTest *QueryServiceTest *QueryIdRpcRoundTripTest"`.
