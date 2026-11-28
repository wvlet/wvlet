# Recover cleanly after an out-of-memory query

## Goals

- After one query's `OutOfMemoryError`, the next query in the same JVM (server, REPL, bench suite) runs at normal speed on an idle heap.
- The bench reports the same RESOURCE_EXHAUSTED error as `WvletEngine.execute`, and the suite keeps going.

## Background

- SF100 baseline: after DS q85's OOM, q86 ran 36x DuckDB on a heap pinned near the cap. At 12 GB, TPC-H q9's OOM killed the test JVM.
- Repro on spark2 (12 GB, DS q01,q85,q86,q87 in one JVM, forced GC before each query): before q86, 11.2 GB was still live and 14 of 20 fair-pool threads were RUNNABLE in `MorselOp.runPart` → `HashJoinOp.probeBatch`, so q85's morsel parts were still probing its 9B-row join.
- Root cause: `MorselOp.close` cancels its in-flight parts, then waits on them with `get()`. `ThreadPoolRunner`'s `Handle.cancel` called `Future.cancel(false)`, which also marks a *running* task cancelled, so `get()` returned at once and the parts ran on after the query had returned. The handle's doc already said "cancel the task if it hasn't started".
- The same pattern (`submit` each, then `get` each) appears in join builds, sorts, windows and aggregations: after the first failure, the caller stops waiting and the remaining tasks run unobserved.
- The bench's `plannedRun` copied `execute`'s body without its OOM conversion.

## Design

Lane: Express. This is a bug fix with a measured diagnosis. Normal-path plans and scheduling don't change, and it touches no always-gate area.

- **Cancel only what hasn't started.** `ThreadPoolRunner` handles (JVM and Native) cancel a task only before it starts. After `cancel()`, `get()` on a running task waits for it to end. Every closing operator that waits on its tasks now does wait.
- **Stop a failed query's tasks early.** On any failure, the query's memory context is marked failed (`MemoryContext.abandon`) before its operators close. Morsel parts check it between output batches, and reservations already check it, so running parts stop within a batch instead of finishing a long probe.
- **One query path.** `WvletEngine.runQuery` plans and runs a query (memory context, abandon on failure, OOM → RESOURCE_EXHAUSTED). `execute` and the bench's `plannedRun` both use it.
- **Don't start the rest after a failure.** `TaskRunner.runAll(runner, n)(f)` replaces "submit each, get each" in join builds, sorts, windows and partitioned aggregation. After a failure, queued tasks never start, and it returns only once the started ones finish. The caller still only waits, as before, so scheduling is unchanged.
- No `Optimization` toggle: the change touches only failure and cancel paths and leaves plans unchanged.
- Tests cover: cancel-then-get on a running task, `runAll` after a failure, and a forced-OOM query followed by a normal one in the same engine. SF100 evidence: the spark2 repro before and after.

## Alternatives and Why Not?

- **Interrupt running tasks (`cancel(true)`).** Operators don't check interrupts, and an interrupt can land in the middle of file I/O (closing NIO channels).
- **Call `System.gc()` or restart the pool after an OOM.** That hides the symptom: the tasks would still run and allocate.
- **Use `inParallel` at the build/sort sites.** It moves work onto the calling thread, which changes scheduling and needs a perf A/B; `runAll` keeps today's behavior.
