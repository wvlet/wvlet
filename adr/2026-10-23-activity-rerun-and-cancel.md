# ADR: Activity reruns are per-policy and cancellation waits for confirmation

Date: 2026-10-23 · PR #2134

## Context

A rerun of an activity stage reuses every terminal item, so a stage retry (`retries:`) reproduces the same failure, and a resumed run cannot get past a cancelled activity stage. Cancellation also interrupted the batch thread, which returned without waiting for items to confirm. Workers that confirmed a cancel and workers that did not looked the same to the caller.

Flow runs also dispatched activity items only when the flow ran on a server. CLI and REPL runs fell back to an in-memory item store, so a resumed CLI run lost the records of its finished items.

## Decision

- Failed items are re-run only when the stage sets `retry_failed: true`. It mirrors `retry_uncertain`, because a failed agent action may already have had side effects, and the two policies stay independent.
- Cancelled items are re-run whenever a cancelled run resumes. `cancelled` is confirmed (no attempt is still running), and a cancelled run is never retried on its own, so the resume is the explicit opt-in.
  - Amended 2026-10-23: only the first stage attempt of a resumed run re-runs them (`BatchConfig.rerunCancelled`). An executor can return `cancelled` on its own, and a stage retry in the same run must not re-run such an item without the resume.
- A cancel stops admission and cancels queued items. Running items get a cancel request and stay `cancelling` until their worker confirms, reports a result, or times out. The batch waits for them for at most the cancel grace period (60s), then returns and leaves the rest `cancelling`. A late result still lands, because `cancelling` is not a final state.
- An activity stage's attempt drains its batch on a flow cancel instead of being interrupted. SQL stages keep the interrupt path. A flow timeout still interrupts, because it is an error, not an operator cancel.
- Every entry point (server `run flow`, CLI `flow run|resume|backfill|scheduler`, and the REPL) records activity items in one item store. The CLI and REPL open `<target>/flow-runs/items.db`, and `WVLET_ITEM_STORE=memory` opts out.
- `FlowRunSummary.cancelRequested` tells a client that a run is still winding down. `cancelRun` keeps its meaning of "requested".

## Consequences

- A stage retry no longer repeats a failure unless the stage asks for it.
- An item that stays `cancelling` after the grace period is resolved only on resume: it is awaited if its executor can resume the attempt, and marked `uncertain` otherwise.
- Cancelling a run does not undo external effects that already happened.
- Every CLI and REPL invocation creates `items.db` under the target folder, including plain queries. The file is small and empty until a flow runs an activity.
- The grace period is a fixed 60s and is not configurable per stage yet.

See [ADR 2026-10-23 — Activity attempt recovery](2026-10-23-activity-attempt-recovery.md) for the invariants this keeps: an attempt that may still run never gets a replacement, and uncertain items are never re-run without `retry_uncertain`.
