# Activity Stages: Queue for Every Entry Point, Failed-Item Re-runs, Batch Cancellation

## Goals

- Every way of starting a flow handles activity stages the same way. Today only server-resumed runs get the server's `TaskQueue`. A `run flow` statement on the server and every CLI or REPL run fall back to an in-memory item store with no queue.
- Retrying a stage, or resuming a run, can re-run failed activity items when the flow opts in. Today stage `retries:` reuses terminal items, so the retry fails the same way again.
- Cancelling a run cancels its activity batch cleanly: admission stops, queued items are cancelled, running items get a cancel request, and the API shows which cancellations are only requested and which are confirmed.
- Keep the ADR invariants (adr/2026-10-23-activity-attempt-recovery.md): uncertain items never re-run without `retry_uncertain`, an attempt that may still be running never gets a replacement, and `reportResult` stays fenced by `attemptId`.

## Background

- **The CLI's in-memory store breaks the ADR.** `wvlet flow run`/`resume` (via `FlowRunLauncher.execute`), the REPL (`WvletCompiler` → `QueryExecutor`) and the scheduler daemon build `FlowExecutor` without an `itemStore`, so they get a fresh `InMemoryItemStore`. When a CLI run is resumed, the store has no item records, so every item runs again, including succeeded and in-flight ones. Fixing this is required by the ADR. It is not just a convenience.
- **Server `run flow`:** `ScriptRunnerSessions` builds `QueryExecutor(connectorProvider, profile, workEnv)`, and `QueryExecutor.executeFlow` / `runEmbeddedFlows` build `FlowExecutor` without a queue. The server's `TaskQueue` is a DI singleton, but only `FlowApiImpl.resumeRun` passes it in.
- **A CLI process has no worker endpoint.** Workers reach the queue only through the server's `TaskApi`. Two processes cannot share one `items.db` as a queue: each `TaskQueue` keeps its pending list in memory and reads the database only at startup, and multi-worker coordination is out of scope.
- **Re-runs today:** `BatchExecutor.execute` reuses every `isTerminal` item (`succeeded | failed | cancelled | uncertain`). With `retry_uncertain`, it re-queues uncertain items at batch start. Failed items have no equivalent. Cancelled items are reused too, so a resumed cancelled run fails its activity stage again with "N cancelled".
- **Cancellation today:** `FlowApi.cancelRun` writes a cancel marker to the run store. `FlowExecutor` polls the marker and calls `cancel()`, which interrupts the worker threads of in-flight stages. Inside the batch, the interrupt runs `handleCancellation()`, which cancels queued items and sends `executor.cancel` to running ones, and then **rethrows right away**. Items that answered `CancelAck.Unknown` stay `cancelling` in the store. Nobody waits for their confirmation, and the API cannot show the difference.

## Design

Lane: **Standard**. This adds a user-visible stage option, changes cancellation behaviour, adds an API field, and makes the CLI write the durable item store (`<target>/flow-runs/items.db`), which is a data-storage-path change.

1. **One item store and an optional queue for every entry point**
   - `QueryExecutor` takes an optional `TaskQueue` and an optional `ItemStore` (added to `QueryExecutorConfig` with `withTaskQueue`/`withItemStore`) and passes both to every `FlowExecutor` it builds. `ScriptRunnerSessions` injects the server's `TaskQueue`, so a `run flow` statement on the server dispatches to workers exactly as a resumed run does.
   - The CLI (`flow run`, `flow resume`, `flow backfill`, `flow scheduler`) and the REPL open `ItemStores.forWorkEnv(workEnv)`, so the store is SQLite by default and `WVLET_ITEM_STORE=memory` opts out. They pass the store as `itemStore` without a queue. The command owns the store and closes it. `FlowRunLauncher.execute` gains an `itemStore` parameter.
   - In the CLI, an activity type without a local executor still fails fast. The message is clearer: such types need the flow to run on a wvlet server (the `run flow` statement or a server resume). The CLI starts no worker endpoint.
   - The server and CLI processes may share `items.db`. Their rows are disjoint by run id, and server recovery adopts only records that have an activity type, which the CLI never writes. We will check that `SQLiteItemStore` sets a busy timeout so concurrent writers wait for each other instead of failing.
2. **Opt-in re-run of failed items: `retry_failed: true`**
   - This is a new stage option next to `retry_uncertain`. When a batch starts (a stage retry under `retries:`, or a resumed run), each `failed` item gets a new `attemptId` and returns to the queue with its attempt count kept. With `retries: N`, every stage attempt re-runs the items that are still failed. The option has no effect on `uncertain` items: `retry_uncertain` alone decides those.
   - Without the option, nothing changes: failed items are reused and the stage fails again.
   - **Cancelled items are re-admitted when a batch starts again, without any option.** `cancelled` means the cancellation was confirmed and no attempt is still running, and a batch only starts again on an explicit resume, because a cancelled run is never retried. Without this, a resumed cancelled run cannot get past its activity stage. A `cancelling` item (cancellation not confirmed) keeps today's path: the batch awaits it if the executor can resume it, and otherwise marks it `uncertain`.
3. **Graceful batch cancellation through `FlowApi.cancelRun`**
   - `FlowExecutor` keeps a reference to the `BatchExecutor` of each running activity stage. `cancel()` calls `batch.cancel()` on those stages instead of interrupting their threads. SQL stages keep the interrupt path.
   - The batch stops admitting items and runs `handleCancellation()`. Queued items become `cancelled`, which is confirmed. Running items get `executor.cancel` and become `cancelled` on `Acknowledged`, or stay `cancelling` on `Unknown`, which means requested. The batch then keeps its event loop running until each `cancelling` item settles: the worker reports a result, the item times out (it becomes uncertain), or its lease expires (also uncertain). The wait has a limit, the cancel grace period, which defaults to the batch lease timeout of 60s. When the grace period ends, the remaining items stay `cancelling` in the store, and the stage ends as cancelled with an error such as `3 cancelled, 1 cancelling`.
   - Slot safety: after a cancel request, nothing more is admitted, and an item whose cancellation is unconfirmed is never re-run. On resume it is awaited, or marked uncertain under the existing rule. A late worker result still lands in the store, because `cancelling` is not a final state.
   - API: `FlowApi.cancelRun` remains the single entry point. It works across processes through the run-store marker, and its answer still means "requested". `FlowRunSummary` gains `cancelRequested: Boolean`, so a client can tell a run that is still winding down from one that has stopped. Confirmation is visible as the run's terminal `cancelled` state and, per item, in `TaskApi.listItems` state counts (`cancelling` = requested, `cancelled` = confirmed). No new TaskApi mutation.
   - If the batch thread is interrupted anyway (for example, at process shutdown), today's behaviour applies: cancel and rethrow without waiting.
4. **Tests.** We use controlled `ActivityExecutor`s and temporary SQLite stores:
   - A resumed CLI-style run reuses succeeded items from a durable store.
   - `QueryExecutor` with a queue dispatches `run flow` activity items.
   - `retry_failed` re-runs only failed items, while uncertain items are re-run only with `retry_uncertain`.
   - A resumed cancelled run re-admits cancelled items.
   - Cancellation confirms queued items and waits for an `Unknown`-ack item until it settles or the grace period ends.
   - An unconfirmed `cancelling` item is never re-run.
   - We also add a DuckDB `.wv` flow spec for `retry_failed`.
   - Docs: website flow page (`retry_failed`, cancellation states). We will add an ADR addendum or a new ADR for the re-run and cancellation rules.

## Alternatives and Why Not?

- **CLI creates its own `TaskQueue` on the durable store.** No worker can poll it, and a server sharing `items.db` would not see the new tasks until it restarts. Without multi-process coordination this only creates tasks that sit in the queue forever.
- **The CLI hosts a worker endpoint, or delegates the run to a server (`--task-server`).** Either would make queue dispatch work from the CLI, but it adds a network surface, auth, and packaging to the CLI. This is better left for a follow-up once there is a concrete need.
- **Re-run failed items by default on retry.** This is simpler, but a failed agent action may have had partial side effects, and the ADR's direction is explicit opt-in. A boolean that mirrors `retry_uncertain` keeps the two policies independent and easy to read.
- **A stage-level `TaskApi.cancelStage(runId, stage)`.** It would cancel one batch while the flow keeps going, but it leaves open what downstream stages should do, and it needs a second signal across processes. Run-level cancellation covers the stated need.
- **Wait for confirmation without a limit.** A local executor with no timeout and no cancel support could then hang the run forever. The grace period bounds the wait, and an unconfirmed item stays visible as `cancelling` instead of becoming a false `cancelled`.
