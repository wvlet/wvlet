# SqlConnector + QueryHandle + cancel (PR #1 of the JDBC-replacement effort)

## Context

`wvlet-runner`'s `DBConnector` is JDBC-shaped (`java.sql.{Connection, Statement, DatabaseMetaData}` plus `io.trino.jdbc.QueryStats` leaking through `QueryProgressMonitor`). It compiles only on JVM and forces native DuckDB (cross-platform work landing) and the new cross-platform Trino HTTP client (added in PR #1708) into adapters that throw away their native row/stats shapes. Investigation of `snowflake-jdbc` confirmed Snowflake is also fundamentally HTTP/JSON underneath, so a JDBC-free interface is the right long-term home for all three backends.

This PR introduces the cross-platform connector interface as a parallel surface — it does **not** rip out `wvlet-runner`'s JDBC `TrinoConnector` (that's a later PR). The goal is to land the abstraction with one concrete `QueryHandle` implementation backed by the existing `Trino.execute` HTTP code, so the design is proven against a real backend on JVM/JS/Native before further migration.

User confirmed `execute` should return a query handle that supports `.cancel()` (the open design question from earlier discussion).

## Scope

In scope:
1. Define `SqlConnector`, `QueryHandle`, `QueryState`, `QueryStats` in `wvlet-lang` (cross-platform).
2. Move `QueryResult` / `QueryResultRow` from `analyzer.duckdb` to the new `compiler.connector` package — they're already shared cross-backend.
3. Refit `Trino` to expose `submit(sql, config): TrinoQueryHandle` with cancel via `DELETE nextUri`. Keep `Trino.execute(sql, config)` as a thin facade (`submit().await()`) so the 5 existing call sites compile unchanged.
4. Add a minimal `TrinoSqlConnector(config)` so the `SqlConnector` trait has one concrete implementation.
5. Replace `wvlet-runner/.../QueryProgressMonitor.scala`'s `TrinoQueryMetric(stats: io.trino.jdbc.QueryStats)` with a cross-platform metric — have `QueryStats extends QueryMetric` directly. Update the JDBC `TrinoConnector.scala:81` to build wvlet `QueryStats` from JDBC `QueryStats`, and update the `WvletREPL.scala:98` consumer.

Out of scope (follow-up PRs):
- `wvlet-runner`'s JDBC `TrinoConnector` migration to use the new SqlConnector trait.
- DuckDB / Snowflake `SqlConnector` implementations.
- Removing the `trino-jdbc` dependency from `build.sbt:417` (PR #1701 becomes moot only after the JDBC `TrinoConnector` is gone).
- Catalog / schema / table listing methods on `SqlConnector`.

## Design

### Types in `wvlet.lang.compiler.connector` (cross-platform)

```scala
trait SqlConnector extends AutoCloseable:
  def submit(sql: String)(using QueryProgressMonitor): QueryHandle
  def execute(sql: String)(using QueryProgressMonitor): QueryResult =
    val h = submit(sql); try h.await() finally h.close()

trait QueryHandle extends AutoCloseable:
  def queryId: Option[String]
  def state: QueryState
  def stats: QueryStats
  def await(): QueryResult     // blocks until terminal, drives pagination, idempotent
  def cancel(): Unit            // best-effort, idempotent, thread-safe vs. await()

enum QueryState:
  case Queued, Planning, Starting, Running, Blocked, Finishing, Finished, Failed, Canceled

case class QueryStats(
  state: QueryState,
  rowsProcessed:   Option[Long] = None,
  bytesProcessed:  Option[Long] = None,
  elapsedMs:       Option[Long] = None,
  splitsCompleted: Option[Int]  = None,
  splitsTotal:     Option[Int]  = None
) extends QueryMetric

object QueryState:
  // Map a Trino `stats.state` string (also used by JDBC) to wvlet's enum.
  // Unknown states fall back to Running (forward-compat with future Trino additions).
  def fromTrino(state: String): QueryState = state.toUpperCase match
    case "QUEUED"    => Queued
    case "PLANNING"  => Planning
    case "STARTING"  => Starting
    case "RUNNING"   => Running
    case "BLOCKED"   => Blocked
    case "FINISHING" => Finishing
    case "FINISHED"  => Finished
    case "FAILED"    => Failed
    case "CANCELED"  => Canceled
    case _           => Running
```

`QueryResult` / `QueryResultRow` move verbatim from `analyzer.duckdb` to `compiler.connector` (4 import sites: `Trino.scala`, `WvletCli.scala`, `QueryResultPrinter.scala`, `QueryResultPrinterTest.scala`).

### `TrinoQueryHandle` lifecycle (in `analyzer.trino`)

- `Trino.submit(sql, config)` blocks on the initial `POST /v1/statement`, parses the response (populates `queryId`, first columns, first rows, first `stats.state`), and returns a `TrinoQueryHandle` holding the `HttpSyncClient`, the latest response JSON, and `@volatile var cancelRequested = false`.
- `await()` runs the existing `while !done` pagination loop, mutating an accumulator. Checks `cancelRequested` at the top of each iteration. Caches the materialized `QueryResult` so a second call returns the same result. After terminal state, transitions to closed.
- `cancel()` is safe from any thread. Sets `cancelRequested=true`, fires `DELETE` on the *last* `nextUri` with the same `X-Trino-*` headers (gateway routing — see comment at `Trino.scala:64-66`), marks state `Canceled`. Idempotent. If the in-flight GET is still running, it completes naturally — the next iteration's flag check exits the loop. Documented latency: cancellation lands at the next pagination boundary.
- `close()`: if not already terminal, calls `cancel()`; then closes the `HttpSyncClient`. Idempotent.
- Per-page stats: a new `collectStats(json, state)` step parses `json.stats.{state, elapsedTimeMillis, processedRows, processedBytes, completedSplits, totalSplits}` and pushes a fresh `QueryStats` via `QueryProgressMonitor.reportProgress`.

### Backward-compat surface

- `Trino.execute(sql, config): QueryResult` stays positional and exception-compatible: it delegates to `submit(sql, config)(using QueryProgressMonitor.noOp).use { _.await() }`. All 5 existing call sites compile unchanged.

### `wvlet-runner` ripples

- Delete `case class TrinoQueryMetric(stats: io.trino.jdbc.QueryStats)` from `wvlet-runner/.../connector/QueryProgressMonitor.scala`. Drops the `import io.trino.jdbc.QueryStats` from that file.
- `TrinoConnector.scala:78-87` (JDBC version, unchanged in shape): the existing `setProgressMonitor` callback receives `io.trino.jdbc.QueryStats stats` — convert to wvlet `QueryStats` inline:
  ```scala
  QueryStats(
    state           = QueryState.fromTrino(stats.getState),
    rowsProcessed   = Some(stats.getProcessedRows),
    bytesProcessed  = Some(stats.getProcessedBytes),
    elapsedMs       = Some(stats.getElapsedTimeMillis),
    splitsCompleted = Some(stats.getCompletedSplits),
    splitsTotal     = Some(stats.getTotalSplits)
  )
  ```
- `WvletREPL.scala:98` change `case m: TrinoQueryMetric => … m.stats.getState …` to `case m: QueryStats => … m.state …`. Unwrap `Option[Long]` with `.getOrElse(0L)` before passing to `ElapsedTime.succinctMillis` and `Count.succinct`.

## Critical files

- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/trino/Trino.scala` — split `execute` into `submit` + `await`; add `TrinoQueryHandle`; parse stats per page.
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/duckdb/QueryResult.scala` — **move** to `wvlet-lang/src/main/scala/wvlet/lang/compiler/connector/QueryResult.scala`.
- New: `wvlet-lang/src/main/scala/wvlet/lang/compiler/connector/SqlConnector.scala` — trait + types.
- New: `wvlet-lang/src/main/scala/wvlet/lang/compiler/connector/QueryHandle.scala` — trait + `QueryState` enum + `QueryStats`.
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/query/QueryProgressMonitor.scala` — leave `QueryMetric` marker as-is.
- `wvlet-lang/.jvm/src/test/scala/wvlet/lang/compiler/analyzer/trino/TrinoTest.scala` — add cancel-path test (fake `HttpServer` records the DELETE), keep existing tests green via the `execute` facade.
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/duckdb/QueryResultPrinter.scala` — import update.
- `wvlet-lang/src/test/scala/wvlet/lang/compiler/analyzer/duckdb/QueryResultPrinterTest.scala` — import update.
- `wvlet-cli-core/src/main/scala/wvlet/lang/cli/WvletCli.scala` — import update.
- `wvlet-runner/src/main/scala/wvlet/lang/runner/connector/QueryProgressMonitor.scala` — remove `TrinoQueryMetric`, drop JDBC `QueryStats` import.
- `wvlet-runner/src/main/scala/wvlet/lang/runner/connector/trino/TrinoConnector.scala` — convert JDBC `QueryStats` to wvlet `QueryStats` in the progress callback.
- `wvlet-cli/src/main/scala/wvlet/lang/cli/WvletREPL.scala` — match on `QueryStats` instead of `TrinoQueryMetric`.

## Verification

- `./sbt scalafmtAll && ./sbt scalafmtCheck`
- `./sbt "langJVM/test"` — TrinoTest passes including new cancel test.
- `./sbt "langJS/Test/compile"` and `./sbt "langNative/Test/compile"` — new connector types compile on all platforms.
- `./sbt "projectJVM/Test/compile"` — runner and CLI still compile after the metric refactor.
- `./sbt "runner/test"` — JDBC `TrinoConnector` progress callback still fires.
- Sanity: `./sbt cli/packInstall && wv 'select 1'` (DuckDB default, unaffected — smoke check no regression).

## Refinements during the PR cycle

These design calls were made during review (codex + Gemini) and are worth capturing for future readers:

1. **`await()` follows `nextUri`, not `stats.state`** (codex P2). Trino's protocol-canonical signal for "done" is `nextUri` absence — a coordinator can report `state: FINISHED`/`FAILED` while still serving a `nextUri` for buffered rows or the final error page. Trusting `state.isTerminal` as a secondary loop exit truncated results in those scenarios. `stats.state` is for progress reporting only; pagination is `nextUri`-driven. Regression test: `await keeps paginating while nextUri is present even if stats.state is FINISHED`.

2. **`cancel()` updates state and notifies the progress monitor immediately** (codex + Gemini). The earlier design only transitioned `_stats.state` to `Canceled` in `await()`'s post-loop block. Threads polling `handle.state` after `cancel()` without awaiting would see stale `Running`, and the REPL status line would never receive the terminal transition through the progress channel. Fix: in `cancel()`, set `_stats = _stats.copy(state = Canceled)` AND call `progressMonitor.reportProgress(_stats)` before firing DELETE. Polling and push paths now agree.

3. **`cancel()` uses a fresh `HttpSyncClient` for the DELETE.** The handle's `client` is consumed by `await()`'s GET loop. uni's `HttpSyncClient` isn't necessarily concurrent-safe (libcurl's `curl_easy` is single-threaded; Node's `worker_threads` channel is single-threaded; only Apache HttpClient is fully concurrent). Spinning up a short-lived second client for the DELETE makes the "safe from any thread" trait promise platform-independent at the cost of one extra connection per cancellation.

4. **Cancel target is `DELETE nextUri`, not `partialCancelUri` or `DELETE /v1/query/{id}`.** Trino exposes three cancel paths in its protocol: (a) `DELETE nextUri` is the documented client-driven abort, releases coordinator resources, transitions state to `CANCELED`; (b) `partialCancelUri` aborts individual stages but lets the query keep running — wrong shape; (c) admin `DELETE /v1/query/{id}` requires elevated privileges. We use (a) and re-apply `X-Trino-*` headers on the DELETE because gateways like Treasure Data's Presto front-end route on those headers per request.
