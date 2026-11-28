# Fair CPU follow-ups: coordinators and yielding (query manager phase 3)

## Goals

- Follow-ups of [fair task scheduling](2026-11-27-fair-task-scheduling.md) (#2576) in the [query manager plan](2026-11-27-query-manager.md): a short query next to a long one still takes a median 1.5–4.8x its alone time.
- Coordinator tasks are picked fairly across queries, and a query waiting on its shared CTEs never stalls behind other queries' coordinators.
- Long-query pool tasks give up their thread to waiting short queries between batches; ship only with a measured win in the mixed bench and no single-query regression.
- Record whether GC pauses now dominate the remaining gap (memory and GC changes belong to another session).

## Background

- Coordinator tasks (`TaskRunner.coordinate`) run on a FIFO pool of half the parallelism. They mostly wait on pool tasks: concurrent join builds (`runTogether`) and shared CTE results (`StartSharedOp`, `computeSharedResults`).
- `runTogether` and `StartSharedOp` readers already run a coordinator task nobody has started on the calling thread. `computeSharedResults` (plan-time CTE results under joins) waited on its coordinator handles with no fallback, so a long query holding every coordinator could stall a short query's planning.
- Pool tasks can't be preempted: a lineitem morsel runs 9–15 ms at SF10 and holds its thread until the part is drained. A task's state lives on its stack (the part's operator pipeline), so it can't be suspended and resumed without rewriting operators.

## Design

- Lane: Standard (runtime scheduling behavior); direction and merge authorization come from the launch prompt.
- `fair_coordinators` (runtime toggle, no plan change, recorded on the query's `TaskGroup`):
  - A query's coordinator tasks run on a second fair pool (its own `FairTaskQueue`, half the parallelism).
  - Coordinator run time is counted apart from pool-task run time, so time a coordinator spends waiting doesn't push its query down a level of the task pool.
  - `computeSharedResults` computes on the calling thread every pending result no coordinator has started, then waits for the rest.
- `EngineMixedWorkloadBench` takes the toggles to A/B (`WVLET_ENGINE_MIXED_TOGGLE`) and logs stop-the-world GC pauses per round.

## Results

- Single query, `WVLET_ENGINE_BENCH_AB=fair_coordinators`, SF1: TPC-DS wall 0.997 (CPU 0.991); TPC-H 1.036, then 0.997 and 0.973 on repeats (noise).
- The mixed bench's long query (a lineitem group-by) uses no coordinators, so it can't show coordinator contention; `EngineConcurrentSharedCTEsTest` shows the liveness fix instead (without the fallback, the query is still blocked after 60 s).

## Yielding: measured, not shipped

- Tried: at each source batch of a morsel part, a task of a yielding query that had run 2 ms took the queue lock and ran inline a queued task of a query clearly ahead of its own (lower level, or under half its run time), at most one level deep, its time not counted to the yielding task.
- SF10 lineitem parts are a single batch each (a debug count: every one of 10,719 yield points was the first and only batch of its part), so the only yield point came right after decode.
- With the batch also cut into 16K-row slices while another query was ahead, 300–450 short-query tasks per round ran inline, but the short query's median next to the long one was 261/232/209/194 ms on vs 146/211/266/181 ms off: no win beyond the noise (machine load 9–10 from other sessions).
- Why it doesn't help: the fair queue already hands the next free thread to the short query, and with 18 threads finishing 16 ms tasks a thread frees up about every 1 ms. Yielding saves that wait, not the 100–200 ms gap.
- The patch is kept in `attachments/wvlet-engine-bench/2026-11-27-yield-to-waiting-queries.patch`.

## GC finding

- Per mixed round (about 4.5 s with both queries running): 29–60 stop-the-world pauses, 0.5–1.4 s in total, the longest 47–187 ms.
- So pauses take 12–30% of the wall time while the queries overlap, about 30–70 ms of a short run's 120–270 ms extra latency: a large share, not the dominant one. The rest is CPU contention, since the long query keeps every thread busy on a machine already at load 7–10.

## Alternatives and Why Not?

- Suspend and re-queue a task at a batch boundary: needs operators that can be resumed on another task.
- Shorter morsels while other queries wait: Parquet parts are row groups with one page per chunk, so a part can't be cut smaller without reading it twice; slicing the decoded batch was the closest variant and showed no win.
- One fair queue for pool and coordinator tasks: coordinators block on pool tasks, so a coordinator on a pool thread could deadlock the pool.
