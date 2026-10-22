# Activity Executor Contract for Wvlet Flow Runner

## Goals

- Promote a public, production-grade activity executor contract so that non-SQL work (agent sessions, external services) can participate in Wvlet flows alongside SQL stages.
- Add queued bounded fan-out: one activity per collection item, with admission control that queues excess items instead of skipping them.
- Persist item/attempt identity and results so that a restarted flow reuses completed work and rejects stale completions from dead workers.
- Establish a cancellation protocol that distinguishes requested cancellation from confirmed termination, and never frees a slot while the original executor is still acting.
- Preserve existing SQL flow behaviour: this is an additive capability, not a rewrite of the stage scheduler.

## Background

Wvlet's `FlowExecutor` already implements a DAG scheduler with bounded parallelism, heartbeats, retries, timeouts, and run-store persistence. Stage execution is SQL-centric: the body of each stage is compiled to SQL and materialized as a run-scoped table via `DBConnector`. The `FlowStageRunner` trait exists as a testing seam — it lets tests substitute the SQL materialization path — but it is not a supported production extension point.

The approved upstream direction (design note 2026-10-22) widens Wvlet's runner into a general-purpose orchestrator. The first vertical slice is a durable bounded-batch contract: one reusable flow, many independent activity invocations, queued admission, bounded concurrency, inspectable results, and cancellation. Treasure Work's agent sessions will be the first non-SQL executor, implemented in a separate adapter PR after this contract is established.

Key constraints from the approved design:
- Agent results need not be SQL tables; they can be structured values or references.
- Excess batch inputs must queue, not become skipped flow starts.
- No automatic retry of uncertain non-idempotent activities; require explicit policy.
- No exactly-once claims for external effects.
- Fencing via attempt identity, not distributed locks.

## Design

Lane: **Standard**. This adds a new runtime API, durable storage extensions, and cross-process execution semantics.

### 1. ActivityExecutor trait

A public contract in `wvlet-runner` that any executor (SQL, agent, external service) implements:

```scala
trait ActivityExecutor:
  def name: String
  def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult
  def cancel(attemptId: String): CancelAck
```

- `ActivityRequest` carries: activity type, invocation id, attempt id, typed input (key-value map), and configuration (timeout, heartbeat interval).
- `ActivityResult` is a sealed type: `Completed(output: Map[String, Any])`, `Failed(error: String, retryable: Boolean)`, `Cancelled`.
- `CancelAck` distinguishes `Acknowledged` (executor will stop) from `Unknown` (executor unreachable).
- The existing SQL materialization path becomes `SqlActivityExecutor`, which wraps the current `materializeStage` logic. `FlowStageRunner` remains as a testing override that delegates through the activity contract.

### 2. Activity stage syntax

Extend the flow grammar with an `activity` keyword for stages whose bodies are not SQL:

```wvlet
flow investigate_customers = {
  stage customers = from customer_list | where priority = 'high'
  stage investigate = from customers | for each _.customer_id
    activity 'agent.investigate'
    with { concurrency: 5, timeout: 30m, heartbeat: 1m }
}
```

- `activity '<type>'` declares the executor type (resolved at runtime from registered executors).
- `for each <expr>` fans out: one activity invocation per distinct value of the expression, with the value bound as `item` in the activity input.
- The parser produces `ActivityStage` and `ForEachFanOut` AST nodes that the typer validates and the executor dispatches.
- Without `for each`, a bare `activity` stage runs once with the stage's input as a single invocation.

### 3. Bounded fan-out with queued admission

`BatchExecutor` orchestrates `for each` stages:

- Maintains a queue of pending items and a pool of active slots (bounded by `concurrency`).
- When a slot completes, the next queued item is admitted immediately — no waiting for all siblings.
- Item states: `queued -> running -> succeeded | failed | cancelled | uncertain`.
- The batch-level concurrency is scoped to this batch invocation, not a process-global pool. The flow-level stage parallelism (`maxParallelism`) governs how many stages run concurrently; within a `for each` stage, the batch concurrency governs how many items run concurrently.

### 4. Durable item/attempt records

Extend `FlowRunStore` with item-level persistence:

```scala
case class ItemRecord(
    itemId: String,       // stable: hash of (runId, stageName, itemKey)
    runId: String,
    stageName: String,
    itemKey: String,      // the for-each value
    state: String,        // queued, running, succeeded, failed, cancelled, uncertain
    attemptId: String,    // ULID, changes on each attempt
    attempts: Int,
    output: Option[String],  // JSON-encoded activity result
    error: Option[String],
    startedAtMillis: Option[Long],
    finishedAtMillis: Option[Long],
    leaseExpiresAtMillis: Option[Long]
)
```

- The SQLite store gets an `items` table alongside the existing `runs` table.
- On restart, completed items are skipped; running items whose lease expired transition to `uncertain`.
- Stale attempt completions (attempt id mismatch) are rejected.

### 5. Recovery and stale-completion fencing

- Each attempt gets a unique `attemptId` (ULID). The store records the current attempt id for each item.
- When `BatchExecutor.reportResult(itemId, attemptId, result)` is called, the store rejects the result if `attemptId` does not match the current attempt (the item was reassigned to a new attempt after the old worker was deemed dead).
- An item whose worker dies mid-execution transitions to `uncertain`, not `failed`. The default policy for agent activities is no automatic retry of uncertain items. The `retry_uncertain: true` config option must be explicitly set.
- SQL activities default to `retry_uncertain: true` since SQL operations are idempotent (CTAS is repeatable).

### 6. Cancellation protocol

- `BatchExecutor.cancel()` stops admitting new items and calls `executor.cancel(attemptId)` for each running item.
- The executor returns `CancelAck.Acknowledged` or `CancelAck.Unknown`.
- Items transition: `running -> cancelling` on cancel request, then `cancelling -> cancelled` on acknowledgement or `cancelling -> uncertain` on timeout.
- A timeout on a running item transitions to `uncertain`, not `failed`, and does not free the slot for a replacement. The slot is freed only when the executor confirms termination or the item's lease expires.

### 7. Inspection states

Add `Queued` and `Uncertain` to `StageState`. Add `ItemState` enum for item-level states. The run store exposes item records for inspection via the existing `FlowApi` endpoints (extended with item-level queries).

## Alternatives and Why Not?

### Temporal as the execution backend
Mature durable execution, but adds a Service and Worker dependency and does not advance Wvlet's own runner. Kept as a future alternative if durability requirements exceed this project's scope.

### Activity calls as activation sinks
The existing `activate` operator delivers output to sinks but does not await results, own attempt identity, or support cancellation. A real activity contract is needed.

### Implementing all Temporal capabilities in one PR
Not a reviewable first slice. This PR establishes the batch/recovery contract; timers, signals, HITL, and multi-worker coordination follow in separate milestones.
