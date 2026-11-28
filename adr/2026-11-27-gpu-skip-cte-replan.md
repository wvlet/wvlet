# ADR: The planner skips plan-time CTE computation for an accelerator that recomputes CTEs

Date: 2026-11-27

## Context

- `replan_materialized_ctes` computes the shared CTEs that joins read at plan time and re-plans those joins with exact row counts (`Planner.replanJoinedCTEs`).
- The GPU executor inlines CTE reads (`inline_ctes`): each GPU reader recomputes the CTE body from cached columns. It declines bare filtered scans, so a CTE whose body is one (TPC-H q15's shared lineitem scan, q22's customer scan) was computed on the CPU at plan time and then never read.
- On one DGX Spark at SF10 this was most of q15 (95 ms) and q22 (70 ms); a JFR profile and a stack capture traced the CPU time to `DeferredCTE.materialize` under `replanJoinedCTEs`.

## Decision

- `PlanAccelerator.recomputesSharedCTEs` (default false) tells the planner that the accelerated subtrees compute the shared CTEs they read. With it and `gpu_offload`, the planner skips the plan-time CTE replan.
- The GPU accelerator sets it when a device is present and both `inline_ctes` and the new `GpuFeature` `skip_cte_replan` are on.
- This is a hook rather than the GPU disabling `replan_materialized_ctes` itself, so CPU plans and the engine's toggles are unchanged, and the GPU side A/Bs it with `WVLET_GPU_BENCH_AB`.

## Consequences

- TPC-H SF10 on the DGX Spark, two one-JVM A/B rounds of 7 hot runs: q15 97.6 to 24.3 ms and 96.0 to 21.6 ms; q22 70.6 to 10.6 ms and 71.6 to 10.7 ms; the suite 737 to 609 ms (0.83x) and engine CPU 0.36x. Other queries moved within noise (q1's 1.10–1.13 in these rounds was 0.96 in a repeat).
- Joins that read a CTE are ordered by estimates rather than exact counts on the GPU path; no TPC-H query got slower. A CTE reader the GPU declines reads the CPU's shared result at run time, without that replan.
- Without an accelerator, or with one that doesn't recompute CTEs, planning is unchanged.
