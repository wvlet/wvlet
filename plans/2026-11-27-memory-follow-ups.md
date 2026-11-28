# Engine memory follow-ups (query manager phase 2)

## Goals

- Finish the follow-ups of [engine memory accounting](2026-11-27-engine-memory-accounting.md) (#2573–#2575), so the server survives concurrent queries that together outgrow the heap.
- The pool's capacity is soft today: queued queries wait, but running queries can keep growing past it until the JVM throws `OutOfMemoryError` and every query fails.
- Reservations count until the query ends, so a long query's finished join builds and sorts look busy and hold back admission.
- `DistinctOp`, set operations, and nested-loop join builds hold rows without counting them.

## Background

- `MemoryPool` is an atomic counter with a soft capacity; each `WvletEngine.execute` opens a `MemoryContext` and closes it in one `finally`. Holders reserve by estimated vector sizes.
- The engine has no general cancellation: operators don't check interrupts, so a query can only be failed where it calls into the engine's own code (reservations, morsel parts, result batches).
- Another session changes `TaskRunner` / `ThreadPoolRunner` scheduling; this work stays out of it.

## Design

Lane: Standard (new server behaviour and option), pre-approved by the launch prompt's goals and merge authorization.

- **Low-memory killer (PR 1).** When the pool stays over capacity for `--low-memory-killer-delay` (default 5 s; `off` disables) and more than one query is open, the pool marks the query holding the most reserved bytes as killed. It fails with RESOURCE_EXHAUSTED at its next reservation, morsel part, or result batch, through the normal error path, so the server emits its COMPLETED event with that status.
  - One victim at a time: no second kill until the killed query closes, then the delay starts again.
  - The pool checks on reservations made while over capacity; the server also checks on a timer, since a query holding its memory without growing never reserves.
  - The message names the query's share, the pool total and capacity, and how to recover (retry, or a larger `--memory-pool-size`).
  - A single query over capacity is not killed: it frees nothing for others, and `--max-query-memory` bounds it.
- **Early release (PR 2).** A holder that drops its rows returns its reservation then: join builds once their probes finish, sort and window buffers once emitted. Shared CTE results after their last reader are a separate follow-up (another session owns it). Each holder keeps its own reservation (a child of the query's context) and releases it when it drops the buffer, never while it still references it.
- **More holders (PR 2).** `DistinctOp` tables, INTERSECT/EXCEPT build sides, and nested-loop join builds reserve like hash joins and aggregations.
- **GC pressure (investigation).** Profile the mixed-workload bench's GC pauses from large aggregations; ship a change only if an A/B shows a win, otherwise record findings in the query manager plan.
- Engine changes stay behind `memory_accounting`; each PR carries an A/B under the bench lock. Tests: a two-query pool over capacity kills the larger and lets the other finish; releases return the pool to its live holders' total mid-query; new holders fail a tiny limit.

## GC pause findings

- Method: SF10 `EngineMixedWorkloadBench` (one round, 20 short q6 runs next to the lineitem group-by), `-Xlog:gc*` from `JAVA_TOOL_OPTIONS`, G1 with a 12 GB heap and 8 MB regions, 14 cores; machine load 10–17 from other sessions' benches.
- Default flags: 342 young pauses, longest 298 ms (an evacuation failure); 15 pauses of 50 ms or more took 1.5 s wall, with 8.2 s system CPU and 2.0 s user CPU. Young pauses freed little (e.g. 3788 → 3716 MB) and promoted about 95 regions (760 MB) of live aggregation state at once.
- `-Xms12g -XX:+AlwaysPreTouch`: worse, with a 450 ms longest pause and 17.6 s system CPU over the long pauses. Committing or first touching heap pages is not the cause.
- `-XX:ParallelGCThreads=4`: longest pause 100 ms, long pauses 0.69 s wall with 1.2 s system CPU; total GC time about the same (3.2 s). Short-query p90 next to the long query was 257 ms, against 274–305 ms in the other two runs.
- Conclusion: the long pauses come mostly from kernel time of 14 GC workers on an oversubscribed machine, not from the engine's allocation volume. No engine change was shipped. Before tuning a server's GC threads, repeat on an idle machine; early release (#2580) and spill shrink the live state that young pauses copy.

## Alternatives and Why Not?

- Kill on the first reservation past capacity: estimates and ahead-reservations overshoot briefly, and a query about to finish would be killed for nothing.
- Interrupt the victim's threads: operators don't check interrupts, and pool threads are shared with other queries.
- Block reservations past capacity until memory frees (Trino's blocked reservations): needs operators that can wait or spill; out of scope.
