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

- The engine shares one FIFO task pool across queries ([phase 3](../plans/2026-11-27-query-manager.md) covers fair CPU sharing).
- Memory: each in-process engine query counts its join builds, aggregation tables, sort and window buffers, shared CTE results, and buffered results against a `MemoryContext` ([engine](engine.md#memory-accounting)) and fails with RESOURCE_EXHAUSTED past its limit. The server does not yet set a limit or share a pool across sessions ([plan](../plans/2026-11-27-engine-memory-accounting.md)).

## Tests

- [`QueryManagerTest`](../wvlet-server/src/test/scala/wvlet/lang/server/QueryManagerTest.scala) holds queries in their slot with a listener that blocks STARTED events, so admission tests don't depend on query run times. With [`QueryServiceTest`](../wvlet-server/src/test/scala/wvlet/lang/server/QueryServiceTest.scala) and the RPC round trip: `./sbt "server/testOnly *QueryManagerTest *QueryServiceTest *QueryIdRpcRoundTripTest"`.
