# Fair CPU sharing across engine queries (query manager phase 3)

## Goals

- A short query running next to a long one on the server should finish in about the time it takes alone, instead of waiting behind the long query's morsel tasks.
- Single-query performance stays unchanged.
- Phase 3 of the [query manager plan](2026-11-27-query-manager.md).

## Background

- Every engine query submits its morsel tasks to one process-wide `ThreadPoolRunner` (`TaskRunner.default`, JVM and Native copies), backed by a fixed pool with one FIFO queue. A long query that has queued hundreds of morsels makes a short query's tasks wait behind all of them.
- Callers already run not-yet-started tasks themselves (`ComputeOnce` in `inParallel` and `runTogether`), so a starved query still makes progress on its own thread, at the speed of one core.
- Tasks are not preemptible: a pool thread runs a task to the end. A scheduler can only choose which queued task runs next.
- `TaskRunner.default` is read in planning (`PhysicalSteps` → `Parallelizer`, which binds operators to the runner) and in shared results, so passing a per-query runner object would touch many call sites and the operators' captured runners.
- Phase 1 runs each server query on its own worker thread; a query's engine work starts from that thread in `WvletEngine.execute`.

## Design

- Lane: Standard (new runtime behavior), with direction and merge authorization given in the launch prompt.
- A task learns its query from a context on the submitting thread: `TaskGroup` (shared code, a `ThreadLocal`). `WvletEngine.execute` opens a new group for the query unless one is already current (nested executes such as `TableWriter` stay in the caller's group).
- `ThreadPoolRunner.submit` and `coordinate` capture the current group; a pool or coordinator thread sets that group while it runs the task, so tasks submitted from tasks stay in the same query.
- The pool keeps one FIFO queue per group and picks the next task across groups with a multilevel feedback queue on each group's run time, like Trino's time-sharing executor:
  - Run time is finished tasks' wall time plus the elapsed time of running tasks, so a new query that has just taken every thread doesn't look idle.
  - Levels by run time (0, 1 s, 10 s, 60 s, 300 s); a group moves down as its tasks' run time adds up.
  - The next task comes from the non-empty level with the least run time relative to its share (each level gets twice the share of the next lower one), so a long query is never starved by a stream of short ones.
  - Within a level, the group that has run least goes first (one task per turn let a query of 15 ms tasks take ~90% of a level shared with 1.5 ms tasks).
- Tasks without a group (tests, tools, the toggle off) go to the old FIFO pool, so the toggle A/Bs the whole scheduler in one JVM.
- Toggle: `fair_task_scheduling`, next to `memory_accounting`. Off, `WvletEngine.execute` opens no group, so every query runs on the FIFO pool as before. It changes no plan text, so it gets a `noPlanChange` entry.
- Coordinator tasks keep their FIFO pool: they mostly wait on pool tasks.
- Validation: a mixed-workload bench (one long TPC-DS query plus a stream of short queries, on vs off) reports short-query latency against running alone; the TPC-H/TPC-DS A/B shows single-query cost.

## Alternatives and Why Not?

- Per-query `TaskRunner` object passed through planning: correct but touches every operator that captures the runner and the shared-results code; the thread context reaches them all without changes.
- Plain round-robin across queries: a short query next to one long query gets half the pool's turns, and less of its time when the long query's tasks are longer.
- Least-accumulated-time first, without levels: an endless stream of short queries would starve a long query.
- Preemptive time slices (Trino's 1 s quanta with split yield): engine tasks cannot yield mid-morsel; morsels are already short.
