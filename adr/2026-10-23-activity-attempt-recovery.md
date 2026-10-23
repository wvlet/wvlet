# ADR: Activity attempt recovery resumes or marks uncertain, never re-executes

Date: 2026-10-23 · PR #2133

## Context

Activity stages (`for each ... activity`) dispatch external work, such as agent sessions, either to a local `ActivityExecutor` or to remote workers through the `TaskQueue`. Both the queue and the batch can restart while an attempt is in flight. When the orchestrator loses contact with a worker, that does not prove the external action failed or stopped. Running the attempt again can duplicate a non-idempotent effect, and leaving it as `running` hides it forever.

## Decision

- The `ItemStore` is the source of truth for dispatch. Enqueue persists a `queued` item with its activity type and input. A poll claims the item atomically (`queued → running`, fenced by `attemptId`). A restarted `TaskQueue` re-queues queued items and re-adopts claimed attempts with their persisted lease and cancel flag.
- A lost worker becomes `uncertain` once its lease expires. The lease is three heartbeat intervals when the task declares a heartbeat, else the queue default. Uncertain items are never retried automatically; `retry_uncertain: true` opts in.
- When a batch restarts, it awaits an in-flight attempt that its executor still tracks (`ActivityExecutor.canResume`). It marks any other in-flight item `uncertain` instead of submitting a new attempt. If a resumed attempt already finished, the batch reports the stored outcome rather than enqueueing it again.
- `reportResult` accepts only the current `attemptId` on an item that is not final. `succeeded`, `failed`, and `cancelled` are final. `uncertain` is terminal for scheduling but not final, so a late result from the same attempt, including one that arrives after lease expiry, can still resolve it.

## Consequences

- A restart never duplicates external work on its own. The trade-off is that uncertain items need user review or an explicit retry policy.
- Before this change, a batch rerun re-submitted in-flight items whose lease had not expired. Those items now become `uncertain` unless their executor can resume them.
- An activity stage fails when any item failed or is uncertain. Stage-level `retries:` reuse terminal items, so a retry reproduces the same failure until an explicit re-run policy exists.
- Executors whose work does not outlive the process keep the default `canResume = false`.
