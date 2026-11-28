# Skip plan-time CTE computation when the GPU recomputes CTEs

## Goals

- Stop the CPU planner from computing shared CTEs at plan time when the GPU readers compute them again, which wasted most of TPC-H q15's (95 ms) and q22's (70 ms) time at SF10 on the DGX Spark.

## Background

- q15's two `revenue` subqueries share one filtered lineitem scan as a deferred CTE; q22 shares a filtered customer scan between its `avg(c_acctbal)` subquery and the main query.
- `replan_materialized_ctes` computes shared CTEs that joins read at plan time, then re-plans the joins with exact row counts (`Planner.replanJoinedCTEs`). A JFR profile and a stack capture showed this CPU scan (`DeferredCTE.materialize`) is the CPU time in q15.
- The GPU declines a bare filtered scan (it returns about as many rows as it reads), so the CTE body runs on the CPU; the GPU readers then inline the CTE body (`inline_ctes`) and recompute it from cached columns, never reading the CPU's result.
- With the GPU on both sides, disabling `replan_materialized_ctes` took q15 from 95.3 to 22.3 ms and q22 from 70.6 to 9.5 ms; no other query moved beyond noise.

## Design

Lane: Express — a performance change on the GPU path only; CPU plans (no accelerator) stay identical, and no always-gate area is touched.

- `PlanAccelerator` gets `recomputesSharedCTEs` (default false). The planner skips the plan-time CTE replan when lowering offers subtrees to an accelerator that says so (`gpu_offload` on).
- The GPU accelerator returns true when a device is present and `inline_ctes` plus a new `GpuFeature` `skip_cte_replan` are on, so the change A/Bs with `WVLET_GPU_BENCH_AB=skip_cte_replan`.
- A CTE reader the GPU declines still reads the CPU's shared result at run time, only without the exact-count replan.
- Checks: `GpuTPCHTest` (spec/tpch and SF10) and `GpuStreamTest` match DuckDB; `EnginePlanAcceleratorTest`; SF10 A/B.
