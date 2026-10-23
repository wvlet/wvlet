# Durable Task Queue and Flow Dispatch for Activities

## Goals

- Make the task-queue worker protocol (#2131) survive a server restart: queued tasks, in-flight attempts, and results live in a durable `ItemStore`, not only in memory.
- Resolve lost workers explicitly: an attempt whose lease expires becomes `uncertain`, never silently retried or left `running` forever.
- Let flow `activity` / `for each ... activity` stages (#2129) actually execute, dispatching through registered executors or the task queue, so one `.wv` flow can fan out agent work to remote workers.
- Give operators an inspection API for item and attempt state with pagination, instead of only the worker-facing poll/result/heartbeat calls.

## Background

- #2129 added the activity contract, `BatchExecutor`, and `SQLiteItemStore`; #2131 added `TaskQueue` and `TaskApi`. They are not connected: the server binds `TaskQueue(InMemoryItemStore())`, `TaskQueue.poll` overwrites the item record (empty run/stage, attempts reset to 1), and `FlowExecutor` never looks at `FlowActivity`/`ForEachFanOut` nodes.
- Recovery semantics come from the approved design: lost worker contact makes an attempt uncertain; no automatic retry unless `retry_uncertain` is set; a timeout or restart must not launch a replacement while the original attempt may still act; fencing is by `attemptId`.
- Open PR #1952 (TypeScript bus + `wvlet-flow-core` prototype) touches no `wvlet-runner/activity` or server files; no code overlap. This PR keeps orchestration in the JVM runner.
- Batch-level stop-admission cancellation is out of scope (separate follow-up); existing cancellation and slot-safety invariants are preserved.

## Design

Lane: **Standard** — durable storage schema and a new server API surface; direction pre-approved (items 1–5).

1. **Durable queue storage**
   - `ItemRecord` gains `activityType`, `input` (JSON object of strings), `timeoutMillis`, `heartbeatMillis`, `workerId`. `SQLiteItemStore` adds the columns, with an additive `alter table` for databases created by #2129.
   - New atomic store operations: `claimItem` (queued → running, fenced by attemptId), `requestCancel` (queued/running → cancelling), `listActiveItems`, paged `listItems` with a state filter, and `countItems` by state.
   - `reportResult` accepts only a matching attemptId on a non-final item (`queued`/`running`/`cancelling`/`uncertain`), so a late result can resolve `uncertain` but never overwrite succeeded/failed/cancelled.
   - `TaskQueue.enqueue` persists the task as `queued` and preserves an existing record's run/stage/key; `poll` claims through the store instead of overwriting it.
   - The server binds `ItemStore.forWorkEnv`: SQLite at `<target>/flow-runs/items.db` by default, `WVLET_ITEM_STORE=memory` to opt out.
2. **Startup reconciliation** — constructing a `TaskQueue` reloads non-final items: `queued` items with an activity type return to the pending queue; `running`/`cancelling` items claimed by a worker are re-adopted as active attempts with their persisted lease (and cancel flag), so a live worker keeps heartbeating and reporting. Nothing is re-executed.
3. **Lease expiry** — `TaskQueue.expireLeases()` turns active attempts past their lease into `uncertain` (error names the worker) and wakes any local waiter. The lease is `3 × heartbeat` when the task declares one, else the default. The server runs the sweep on a daemon timer.
4. **Flow dispatch**
   - `FlowLowering` records an `ActivitySpec(activityType, forEachKey)` per stage and strips the activity node from the SQL body.
   - `FlowExecutor` takes `activityExecutors` and an optional `taskQueue`. An activity stage evaluates its input, collects distinct `for each` keys (`item` input), and runs a `BatchExecutor` with the stage's `concurrency`, `timeout`, `heartbeat`, and `retry_uncertain`. An activity type without a local executor dispatches through the task queue (`TaskQueueActivityExecutor`); with neither, the stage fails as not implemented.
   - The stage table holds one row per item (`item_key, state, attempts, output, error`) so downstream stages and inspection read results. The stage fails when any item failed or is uncertain.
   - The server passes its task queue to the runs it resumes (`FlowRunLauncher`); starting runs from the server with a queue is follow-up work.
   - Stage-level timeout/heartbeat watchdogs do not apply to activity stages; those settings bound each item instead.
   - Restart safety: `ActivityExecutor.canResume(attemptId)` (default false). On rerun, `BatchExecutor` re-awaits an in-flight attempt the executor still tracks; otherwise an in-flight item becomes `uncertain` instead of being re-executed. An interrupted batch marks queued items cancelled and sends cancellation to running ones before exiting.
5. **Inspection API** — `TaskApi` adds `listItems(runId, stageName, state?, offset, limit)` returning a page plus per-state counts, `getItem(itemId)`, and `listActiveAttempts`. The responses expose identities, states, worker id, timestamps, output, and error; nothing is derived from model narration.

## Alternatives and Why Not?

- **Re-run uncertain or in-flight items on restart.** Simple, but may duplicate non-idempotent agent work. Rejected by the approved design; opt-in through `retry_uncertain`.
- **Separate inspection RPC service.** Cleaner split between worker and operator calls, but needs new routing and a client for little gain now. Inspection lives on `TaskApi` and can move later.
- **Per-attempt history table.** Better audit, but no retry path produces multiple attempts by default. Deferred; the item record holds the current attempt and the attempt count.
