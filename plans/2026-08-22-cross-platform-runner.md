# Cross-platform wvlet-runner: DuckDB + REST backends on JVM / JS / Native

Date: 2026-08-22

## Goal

Design `wvlet-runner` so query execution works on all three platforms (JVM, Node.js,
Scala Native), with DuckDB as the embedded engine and REST-based backends (Trino today,
a remote wvlet-server next) as the network engines. Identify what is missing and define
the path to close the gaps.

## Verified current state (2026-08-22)

There are two independent execution stacks:

| | JVM stack | Cross-platform stack |
|---|---|---|
| Entry | `wvlet-cli` / `wvlet-server` → `wvlet-runner` | `wvlet-cli-core` (`wvc`, `@wvlet/cli` on Node, `cliCoreJVM`) |
| Executor | `QueryExecutor` (full `ExecutionPlan` interpreter, 1200+ lines) | `WvletCli.run` — compile to a single SQL string, execute, print |
| Engine access | `Connector`/`DBConnector` family in `wvlet-connector` (DuckDB JDBC, Snowflake JDBC, Trino REST, Slack) | `DuckDB.execute` (JDBC / koffi / libduckdb C API) and `Trino.execute` (uni `HttpSyncClient`) in `wvlet-lang` |
| Features | multi-statement, `test`, `use`, save-to, models, flows, staging, catalogs | single statement, box/CSV printing |

Smoke-verified in this exploration:

- **DuckDB execution works on all three platforms** — `wvc run` (native binary,
  libduckdb via `@extern` bindings + C shim), `node sdks/cli-node/bin/wvlet.js run`
  (koffi FFI, `WVLET_LIBDUCKDB`), and `cliCoreJVM/run` (JDBC) all execute
  `from [[1,'x']] as t(id, msg) select …` and print the box result.
- **Trino REST works end-to-end from native and Node binaries** — verified against a
  local fake coordinator implementing `POST /v1/statement` + `nextUri` pagination;
  the 41 JVM Trino protocol tests (fake-server suite) all pass.
- **Confirmed feature gaps in the cross-platform path** (same native binary):
  - `test _.size should be 1` after a query is **silently ignored** — no test evaluation.
  - A two-statement script (`select 10 as x; select 20 as y`) **runs only the first
    statement**; the rest are dropped.
  - Every `DuckDB.execute` opens a **fresh in-memory database** — temp tables, models
    materialized as views, and `use`-style session state cannot survive across
    statements (`DuckDBSqlConnector` scaladoc documents this).

## Gap analysis

### G1. No execution-plan interpreter outside the JVM
`wvlet-cli-core/src/main/scala/wvlet/lang/cli/WvletCli.scala:33-102` compiles to one
SQL string and dispatches on a hardcoded `"duckdb" | "trino"` match. Everything
`QueryExecutor.execute(ExecutionPlan, Context)` handles
(`wvlet-runner/src/main/scala/wvlet/lang/runner/QueryExecutor.scala:254-351`) —
`ExecuteQuery`, `ExecuteTest`, `ExecuteCommand` (`use`, `show`, `describe`, `explain`),
`ExecuteValDef`, `ExecuteSave`, multi-statement sequencing — is JVM-only.

### G2. `wvlet-runner` and `wvlet-connector` are structurally JVM-locked
Both are plain sbt `project`s (`build.sbt:433-485`). Bindings: `java.sql.*`
(`DBConnector`), caffeine (`ConnectorCatalog`), jline (`QueryResultPrinter` uses
`org.jline.utils.WCWidth`), arrow-vector, sqlite-jdbc (`SQLiteFlowRunStore`),
`java.util.concurrent` (scheduler, thread pools). Flows/scheduler/REPL are
legitimately JVM features — the crossProject conversion must keep them in
`.jvm/src`, not port them.

### G3. No persistent DuckDB session on JS/Native
`DuckDBCompat.execute` (all three platforms) opens a database, runs one statement
batch, closes. The JVM runner solved this with a stateful
`DuckDBConnector.asSqlConnector` view over its long-lived JDBC connection (#1919);
JS/Native have no equivalent. This blocks models, temp tables, `use`, and
multi-statement scripts on those platforms even once G1 is fixed.

### G4. The wvlet-server REST API cannot serve as a query backend
- `QueryService.runQuery` fills only `preview: String` (pretty-box); `QueryInfo.result`
  (structured rows) is never populated (`wvlet-server/.../QueryService.scala:80-87`).
- Pagination is stubbed (`pageToken` counts "0"/"1"/"2"; `// TODO Support pagination`).
- `FrontendApi` has no cancel endpoint, although `QueryHandle.cancel()` exists
  end-to-end for Trino.
- `QueryRequest.profile` is ignored server-side (only `sessionId` is used).
- `wvlet-client` cross-builds JVM+JS only (`build.sbt:601-605`); no Native RPC client,
  though `wvlet-api` already builds for Native.

### G5. Trino client is not production-ready
Only `X-Trino-User` is sent; no basic auth, JWT, or OAuth. No TLS options beyond
`useHttps`. (Design decision record: `plans/2026-05-14-trino-cross-platform.md`.)

### G6. Browser is a different execution model
`HttpSyncClient` on JS is Node-only (`worker_threads` + `Atomics.wait`); the browser
playground executes via DuckDB-WASM's async API with its own result path
(`wvlet-ui-main/.../duckdb/DuckDBWasm.ts`). The sync `SqlConnector` trait cannot be
implemented in a browser. Browser execution stays a separate consumer (playground) in
this design; an `AsyncSqlConnector` mirror is future work.

## Target architecture

> **Revision (2026-08-22, user feedback):** no separate `wvlet-runner-core` module.
> `wvlet-runner` itself becomes a `crossProject(JVM, JS, Native)` with
> `CrossType.Pure` — the same layout `wvlet-lang` uses: shared code stays in
> `wvlet-runner/src`, and the JVM-only pieces move into `wvlet-runner/.jvm/src`.
> The JVM platform keeps its `wvlet-connector` dependency via `.jvmConfigure`
> (per-platform `dependsOn`), so no new artifact and no package renames are needed.

Convert `wvlet-runner` to a crossProject whose shared sources hold the
engine-agnostic runtime, consumed by both the JVM CLI/server stack and the thin
Node/Native CLIs:

```
  wvlet-cli (JVM: REPL, ui, flows)          wvc (Native)      @wvlet/cli (Node)
        │                                          │                │
        │                                          └───────┬────────┘
        │                                                  │
        │                                          wvlet-cli-core (X)
        │                                                  │
        └────────────────┬─────────────────────────────────┘
                         │
              wvlet-runner (converted to crossProject JVM/JS/Native, CrossType.Pure)
                src/ (shared):
                  PlanExecutor   — ExecutionPlan interpreter over SqlConnector
                  TestEvaluator  — `test` statement evaluation (moved from QueryExecutor)
                  QueryResult ADT + printer (jline-free width handling)
                  SqlConnectorProvider — ConnectorConfig → SqlConnector registry
                .jvm/src/ (JVM-only, depends on wvlet-connector via .jvmConfigure):
                  QueryExecutor (extends PlanExecutor), flows, scheduler, run stores,
                  staging, activation sinks, arrow, jline REPL support
                         │
              wvlet-lang (X, existing)
                SqlConnector / QueryHandle / QueryState / QueryStats
                DuckDB compat (JDBC / koffi / C API) + DuckDBSqlConnector + DuckDBSession
                Trino REST client + TrinoSqlConnector
                Profile / ConnectorConfig (cross-platform JSONC + env expansion)
```

`SqlConnector` (sync, cross-platform, `wvlet-lang/.../compiler/connector/SqlConnector.scala`)
remains the single engine abstraction. The JVM `Connector` capability family
(`wvlet-connector`) stays as-is; its `DBConnector.sqlConnector: Option[SqlConnector]`
bridge is how JVM-only engines join the shared interpreter later.

### C1. Persistent DuckDB sessions (fixes G3)

Add a session API to the `DuckDBCompat` facade:

```scala
trait DuckDBSession extends AutoCloseable:
  def execute(sql: String): QueryResult   // runs on the SAME connection
object DuckDB:
  def newSession(path: Option[String] = None): DuckDBSession
```

- JVM: wrap one `jdbc:duckdb:` connection (pattern already proven by
  `DuckDBConnector.asSqlConnector`, #1919).
- Native: hold `duckdb_database` + `duckdb_connection` across calls; today's
  open→query→close body in `DuckDBCompat.execute` (`wvlet-lang/.native/.../DuckDBCompat.scala:93`)
  becomes the session's `execute` with open/close moved to the session lifecycle.
- JS: same restructuring over the koffi handles.

`DuckDBSqlConnector` gains a session-backed mode (constructor takes a session;
`close()` closes it). The stateless one-shot behavior remains available for
single-query calls.

### C2. Cross-platform `SqlConnectorProvider` (partial G1, enables profiles)

In shared `wvlet-runner` sources: resolve a `ConnectorConfig` (from the cross-platform
`Profile`) to a `SqlConnector`:

| `type` | Connector | Availability |
|---|---|---|
| `duckdb` | session-backed `DuckDBSqlConnector` | where `DuckDB.canExecute` |
| `trino` | `TrinoSqlConnector(TrinoConfig)` | all platforms (Node/Native/JVM) |
| `wvlet` (new) | `WvletServerSqlConnector` → remote wvlet-server RPC | after C4 |
| others (`snowflake`, `slack`, …) | error naming the type and pointing at the JVM CLI | JVM runner keeps its full `ConnectorProvider` |

Single-threaded CLI usage → plain `Map` cache keyed by `ConnectorConfig` value
equality (same key rule as the JVM provider, decision D5 of
`plans/2026-07-04-multi-connector-profiles-phase1.md`).

### C3. `PlanExecutor`: extract the engine-agnostic interpreter (fixes G1)

Split `QueryExecutor` along the `SqlConnector` seam:

- **Moves to shared `wvlet-runner/src`** (verified platform-clean):
  - The `ExecutionPlan` walk (`execute`/`process`/`report`,
    QueryExecutor.scala:254-351) for `ExecuteQuery`, `ExecuteStatement` lists,
    `ExecuteTest`, `ExecuteValDef`, `ExecuteCommand` subset (`ExecuteExpr`,
    `ExplainPlan`, `ShowQuery`, `UseSchema`/`UseConnector`), `ExecuteNothing`.
  - `executeTest` (QueryExecutor.scala:943-1222) — pure expression evaluation over the
    last result; no JDBC anywhere.
  - The runner `QueryResult` ADT (`wvlet-runner/.../QueryResult.scala` — imports only
    uni + wvlet-lang types; verified).
  - `QueryResultPrinter` — needs a jline-free `WCWidth`. Either a small pure-Scala
    width table or reuse of the simpler cross-platform printer already in
    `wvlet-lang/.../duckdb/QueryResultPrinter.scala`, extended with the runner's
    formats.
- **Stays in `wvlet-runner` (JVM)**, as overrides on the core interpreter:
  `ExecuteSave` (local-file staging via `java.io` + DuckDB COPY), `ExecuteFlow`,
  `ExecuteDebug`, `ExecuteTasks`, embedded flows/tool calls
  (`runEmbeddedFlows`/`runEmbeddedToolCalls`), source-table staging across
  connectors, `DBConnector`/JDBC result paths, arrow export, catalog registration.

Shape: `class PlanExecutor(connectorProvider, profile)` in shared runner sources with
`protected def executeUnsupported(plan): QueryResult` hooks;
`QueryExecutor extends PlanExecutor` overrides them with the JVM implementations.
`wvlet-cli-core`'s `run` switches from "generate one SQL string" to
"compile → `PlanExecutor.execute(plan, ctx)`", which immediately fixes the dropped
statements and silent `test` skips on all platforms, and puts spec-driven `.wv`
runs (RunnerSpec-style) within reach of JS/Native CI.

### C4. wvlet-server as a first-class REST backend (fixes G4)

Server side:
1. Populate `QueryInfo.result` with the structured rows (bounded by
   `QueryRequest.maxRows`) and implement real page tokens.
2. Add `FrontendApi.cancelQuery(QueryCancelRequest)` wired to the runner's
   cancellation (Trino `QueryHandle.cancel` already works; DuckDB via
   `executeCancellable`).
3. Honor `QueryRequest.profile` when resolving the session's connector provider.

Client side:
4. Add `NativePlatform` to the `wvlet-client` crossProject — it depends only on
   `wvlet-api` + uni RPC, both of which already cross-build to Native.
5. New `WvletServerClient` in shared runner sources: submit → poll `getQueryInfo` →
   adapt the structured result; cancel → `cancelQuery`. `-t wvlet` on the CLI
   sends the **original wvlet text** and skips local compilation entirely.
   > **Revision (phase-5 implementation):** not a `SqlConnector`. Wrapping the
   > server in `SqlConnector.submit(sql)` and routing through `PlanExecutor`
   > would compile locally, generate SQL, and have the server re-compile that
   > text — double compilation with mismatched contexts. The server owns the
   > compilation (its profiles, catalogs, credentials), so the client stays a
   > raw-text remote runner with one server session per client instance.

This gives thin native/Node clients a REST path to every engine the JVM server
supports (including Snowflake JDBC and multi-connector staging) without porting any
JDBC driver — the second "REST-based backend" besides Trino.

### C5. Trino production auth (fixes G5)

`TrinoConfig` gains `password: Option[String]` and `token: Option[String]`; the
client sends `Authorization: Basic …` (password requires `useHttps`) or
`Authorization: Bearer …`. Values come from `ConnectorConfig.password` /
`properties.token` with `${ENV}` expansion already handled by the profile loader.

### C6. Explicit non-goals

- **Flows, scheduler, run stores, REPL, server hosting** stay JVM-only.
- **Snowflake on JS/Native** — deferred; the eventual path is Snowflake's SQL REST
  API as another `SqlConnector`, not the JDBC driver.
- **Browser execution** — stays on DuckDB-WASM in the playground; a future
  `AsyncSqlConnector` could unify it, but sync `SqlConnector` is Node/Native/JVM only.
- **Catalog metadata on JS/Native** — `ConnectorCatalog` (caffeine) stays JVM; the
  cross-platform path compiles catalog-free (as `wvc`/Node do today) or against the
  static catalog files.

## Phased delivery (PR-sized)

1. **PR1 — DuckDB sessions (C1)**: `DuckDBSession` on all three platforms +
   session-backed `DuckDBSqlConnector` + cross-platform tests (temp table survives
   across `execute` calls; gated on `DuckDB.canExecute` like `DuckDBExecuteTest`).
2. **PR2 — runner crossProject conversion + provider (C2)**: convert
   `wvlet-runner` to `crossProject(JVM, JS, Native)` with `CrossType.Pure`,
   moving the JVM-only sources into `.jvm/src` (mechanical, no package changes);
   keep the runner `QueryResult` ADT + a jline-free printer in shared sources;
   add `SqlConnectorProvider`; route `WvletCli.run` through it (profile-driven
   backend selection preserved, `-t` still wins).
3. **PR3 — PlanExecutor extraction (C3)**: the big refactor. `QueryExecutor` keeps
   its public surface; JVM `runner/test`, RunnerSpec suites, and TyperCoverageCheck
   guard the move. cliCore `run` executes plans; add native/Node smoke specs for
   multi-statement + `test` statements.
4. **PR4 — server REST completion (C4 server half)**: structured results, pagination,
   cancel endpoint, per-request profile.
5. **PR5 — native RPC client + `wvlet` connector (C4 client half)**: `wvlet-client`
   Native platform; `WvletServerSqlConnector`; `wvc run -t wvlet --host …` smoke
   against a local `wvlet ui` server.
6. **PR6 — Trino auth (C5)**: basic + bearer; fake-server tests for the header paths.

Each PR verifies: `./sbt "projectJVM/Test/compile" "projectJS/Test/compile"
"projectNative/Test/compile"`, `./sbt runner/test`, `./sbt "langJVM/test"`, and for
PR2+ the three-platform smoke (`cliCoreJVM/run`, `node sdks/cli-node/bin/wvlet.js`,
`wvc`) documented above.

## Risks

- **QueryExecutor entanglement**: `Context`/catalog access inside the interpreter may
  drag JVM-only catalog types; the seam must pass capabilities in, not reach out
  (mitigation: PR3 lands behind the existing JVM test suites, which are extensive).
- **`wvlet-runner` published for 3 platforms** (was JVM-only): release automation
  aggregates `projectJVM/JS/Native`, so inclusion is automatic, but npm/native binary
  size will grow — watch `wvc` link time and `@wvlet/cli` bundle size.
- **uni `HttpSyncClient` thread-safety** — single-threaded on Node/Native; the
  interpreter must stay single-threaded per connector (it is today).
- **DuckDB session lifetime on Native** — open handles now outlive a `Zone`; the
  session must own its allocations and be closed deterministically (CLI `finally`).
