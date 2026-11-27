# Server

`wvlet-server` runs queries for the UI, the `wvlet` CLI's server client, and SDKs over the HTTP API. See the [system map](../ARCHITECTURE.md) for where it sits.

## Query lifecycle

- [`QueryService`](../wvlet-server/src/main/scala/wvlet/lang/server/QueryService.scala) issues a ULID per query and tracks QUEUED → RUNNING → FINISHED / FAILED / CANCELED ([`QueryStatus`](../wvlet-api/src/main/scala/wvlet/lang/api/v1/query/QueryStatus.scala)). Clients poll `fetchNext` by id.
- Cancel is best effort: the status becomes CANCELED at once and later completion is discarded; a query cancelled while queued never starts.
- [`ScriptRunnerSessions`](../wvlet-server/src/main/scala/wvlet/lang/server/ScriptRunnerSessions.scala) gives each client session its own runner, so `use` changes only that session. Statements of one session are serialized because the compiler is not thread-safe; sessions run in parallel. Idle sessions are evicted after an hour.

## Limits

- The server does not yet bound concurrent or queued queries, keep finished queries for a bounded time, or log query events. The engine shares one FIFO task pool across queries and has no memory budget. The [query manager plan](../plans/2026-11-27-query-manager.md) covers admission, events, retention, memory accounting, and fair CPU sharing.

## Tests

- [`QueryServiceTest`](../wvlet-server/src/test/scala/wvlet/lang/server/QueryServiceTest.scala) and [`ScriptRunnerSessionsTest`](../wvlet-server/src/test/scala/wvlet/lang/server/ScriptRunnerSessionsTest.scala): `./sbt "server/testOnly *QueryServiceTest *ScriptRunnerSessionsTest"`.
