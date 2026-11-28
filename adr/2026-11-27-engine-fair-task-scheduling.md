# ADR: Engine pool tasks carry their query's task group and run by least attained time

Date: 2026-11-27 · PR #2576

## Context

All engine queries in a process share `TaskRunner.default`, a fixed pool with one FIFO queue, so a short query on the server waited behind a long query's queued morsels. `TaskRunner.default` is read while planning (`Parallelizer` binds operators to it) and by shared results, so a per-query runner object would have to be threaded through planning and every operator that captures the runner. Engine tasks can't be preempted, and TPC-H/TPC-DS queries run well under a second at SF10, while their tasks run 1–15 ms.

## Decision

- A task belongs to the `TaskGroup` current on the thread that submits it, a thread-local like `MemoryContext`. `WvletEngine.execute` opens a group per query (unless one is current); pool and coordinator threads make the task's group current while it runs.
- Grouped tasks run on a second pool whose `FairTaskQueue` is a multilevel feedback queue, as in Trino's time-sharing executor: levels at 0, 1, 10, 60, and 300 s of a group's run time, level shares halving per level, and a newly busy level starting at the busy levels' least weighted time.
- A group's run time counts its running tasks' elapsed time, and within a level the group that has run least goes first.
- Ungrouped tasks keep the FIFO pool, so `fair_task_scheduling` A/Bs the whole scheduler in one JVM.

## Consequences

- No planner or operator signature changed; any new code that submits tasks from a thread it starts itself must set the group, or its tasks fall back to the FIFO pool.
- Single-query cost is unchanged (TPC-H 0.99, TPC-DS 1.00 wall at SF1), after moving submitters off the workers' lock.
- Short queries next to a long one improve (p90 ~1.5x lower in `EngineMixedWorkloadBench`) but don't reach their alone latency: running long-query tasks finish first, coordinators stay FIFO, and GC pauses stall every query.

## Alternatives

- A per-query `TaskRunner` passed through planning: same effect, many more touched call sites.
- Round-robin across queries, or within a level one task per turn: time-unfair when one query's tasks are 10x longer.
- Ordering by finished tasks only: a new long query looks idle until its first tasks finish and takes every thread.
