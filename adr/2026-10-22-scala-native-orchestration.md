# ADR: Cross-platform orchestration in Scala (JVM + Scala.js)

Date: 2026-10-22 · PR #2129 · Design note: `plans/2026-10-22-activity-executor-contract.md`

## Context

Wvlet's flow runner already owns DAG scheduling, stage dependencies, and SQL materialization. PR #2129 adds an `ActivityExecutor` contract for non-SQL stages (agent sessions, external service calls) with durable item/attempt tracking, bounded fan-out, heartbeat, and cancellation. The open question is how non-JVM workers — particularly TypeScript, which owns Treasure Work's agent execution — connect to this orchestration.

Three paths were considered:

1. **Adopt Temporal.** Deploy a Temporal cluster; Wvlet compiles flows into Temporal workflows. JVM and TypeScript workers use Temporal's native SDKs.
2. **JVM-only orchestration server with a TypeScript facade SDK.** Wvlet's flow executor becomes the orchestration server on JVM. TypeScript connects via HTTP with a typed facade.
3. **Cross-platform orchestration in Scala.** The orchestration server is cross-platform Scala that compiles to both JVM and Scala.js. Both platforms run the same server code; workers connect over the same protocol on either platform.

## Decision

**Cross-platform orchestration in Scala (option 3).** Uni already provides an HTTP server that runs on both JVM and Scala.js. The flow compiler is already cross-platform. The scheduling logic — DAG resolution, state machine transitions, task queue admission, heartbeat monitoring, stale-completion fencing — is pure logic with no inherent JVM dependency. By keeping the orchestration core cross-platform, the same server can run on JVM (production, heavy workloads, SQLite persistence) or Node.js via Scala.js (lightweight/embedded deployments, TypeScript-native environments) from a single Scala source.

Temporal was rejected because mapping Wvlet's flow semantics onto Temporal's opinionated abstractions (workflow replays, determinism constraints, signal channels) adds integration complexity without adding capability this system needs. A JVM-only server (option 2) was rejected because it forces a JVM deployment even in TypeScript-native environments and turns the TypeScript SDK into a second-class client rather than a peer.

### Cross-platform split

The orchestration kernel is cross-platform Scala. Platform-specific code is behind traits with platform implementations:

| Layer | Cross-platform | Platform-specific |
|---|---|---|
| Flow compiler, DAG scheduler | Yes (already) | — |
| State machine (run/stage/item/attempt transitions) | Yes | — |
| Task queue logic, admission control, fencing | Yes | — |
| HTTP endpoints (Uni HTTP server) | Yes | — |
| `ActivityExecutor` trait and `ActivityRequest`/`ActivityResult` types | Yes | — |
| Executor loop (concurrency) | Trait | JVM: blocking threads / JS: async event loop |
| Persistence (`ItemStore`) | Trait | JVM: SQLite (JDBC) / JS: pluggable (IndexedDB, HTTP-backed, in-memory) |

The JVM `BatchExecutor` currently uses `java.util.concurrent.*` (LinkedBlockingQueue, ConcurrentHashMap, ScheduledExecutorService). The cross-platform refactor extracts the scheduling logic into a platform-independent core and provides:
- **JVM implementation**: blocking thread pool, same as current `BatchExecutor`
- **Scala.js implementation**: async/callback-based executor using JavaScript's event loop, where `poll` and `heartbeat` are scheduled with timers and item dispatch is non-blocking

### Architecture

```
┌──────────────────────────────────────────────────────┐
│         Orchestration Server (cross-platform Scala)  │
│  ┌───────────┐  ┌──────────┐  ┌───────────────────┐ │
│  │ FlowExec  │  │ Task     │  │ ItemStore trait    │ │
│  │ (DAG)     │──│ Queue    │  │ (platform impl)   │ │
│  └───────────┘  └────┬─────┘  └───────────────────┘ │
│                      │ Uni HTTP                      │
└──────────────────────┼───────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │ JVM          │         JS   │
  ┌─────┴─────┐  ┌────┴────┐  ┌─────┴──────┐
  │  Server   │  │ Workers │  │  Server    │
  │  (JVM)    │  │  (any)  │  │  (Node.js) │
  │  SQLite   │  │         │  │  pluggable │
  └───────────┘  └─────────┘  └────────────┘
```

Both the JVM and Node.js server deployments expose the same HTTP endpoints. Workers do not know or care which platform the server runs on.

### Worker protocol (HTTP)

A single protocol for all workers regardless of language or server platform:

- **`POST /v1/tasks/poll`** — long-poll for a task matching the worker's declared activity types. Returns an `ActivityRequest` or empty after timeout.
- **`POST /v1/tasks/{attemptId}/result`** — report completion, failure, or cancellation. Returns accepted or fenced.
- **`POST /v1/tasks/{attemptId}/heartbeat`** — renew the liveness lease. Returns continue or cancel.

### SDKs

- **JVM SDK** — wraps the `ActivityExecutor` trait. For co-located deployments (server and worker in the same JVM), dispatch skips HTTP and calls the executor directly.
- **TypeScript SDK** — Scala.js compilation of the same cross-platform worker code, published as an npm package. Not a hand-written facade — the same `ActivityExecutor` trait, `ActivityRequest`/`ActivityResult` types, and poll/dispatch loop, compiled to JavaScript. TypeScript activity handlers implement the executor interface and register with the SDK.

Type generation is unnecessary: the TypeScript SDK's types are the Scala types compiled to JavaScript, so they stay in sync by construction.

## Consequences

- **One source, two platforms.** The orchestration logic is written once in Scala and runs on both JVM and Node.js. Bug fixes and features apply to both platforms without synchronization.
- **No external infrastructure dependency.** No Temporal cluster. The orchestration server is part of the Wvlet deployment on whichever platform the deployer chooses.
- **TypeScript is a first-class platform, not a client.** A Node.js deployment runs the full orchestrator — compiles flows, schedules stages, dispatches activities — without a JVM. This matters for environments where JVM is unavailable or undesirable.
- **Platform-specific code is scoped to two traits.** `ItemStore` (persistence) and the executor loop (concurrency) are the only platform-specific implementations. Everything else — including the state machine, fencing logic, and HTTP endpoints — is shared.
- **The async refactor is the main cost.** The current `BatchExecutor` is blocking/thread-based. Making the executor loop cross-platform requires an async abstraction that maps to threads on JVM and the event loop on JS. This is a refactor of the concurrency model, not the scheduling logic.
- **We own the complexity.** Features Temporal provides out of the box — multi-worker partitioning, task-queue sharding, durable timers, signals — must be built when needed. Single-server task queues and the existing `ItemStore` persistence are sufficient for the first production use case.
- **The `activity` and `for each` syntax (PR #2129) is independent of the dispatch mechanism.** The compiler produces `FlowActivity` and `ForEachFanOut` plan nodes; the executor decides whether to dispatch in-process or over the task queue. Switching dispatch does not require recompilation.
