# GPU offload for new CPU plan shapes

## Goals

- Bring TPC-H q15 and q22 back onto the GPU executor (`wvlet-engine-gpu`) after recent CPU planner work moved them off it.
- Make the next such regression visible without a GPU: CI never builds `engineGpu`, and the Nov 27 DGX Spark run found these three only by comparing totals (GPU total 720 ms on Nov 24, 1,176 ms on main; q10 35 → 326 ms, q15 25 → 109 ms, q22 11 → 100 ms).

## Background

- The GPU executor takes a runtime-plan subtree only when every node in it is one it runs (`GpuPlans.supports`); anything else stays on the CPU, with no message.
- #2464 made q15 and q22 read their filtered scans through a shared CTE (`CTERef`), computed once for a plan-time scalar subquery and the main query. The GPU didn't know `CTERef`, so their joins and aggregations moved back to the CPU.
- q10's runtime plan still offloads whole: the eager aggregation (#2480) and 20-row string re-read (#2499) happen in CPU lowering, below the accelerator. Its slowdown has another cause, which the Spark profile run will name.
- The GPU keeps scanned Parquet columns cached on the device, so recomputing a filtered scan per reader costs little there.

## Design

- Lane: Express. An internal change to the example GPU module plus a test, with no user-facing behaviour and no always-gate area.
- A shared-CTE read runs on the GPU as the CTE body, renamed to the reader's columns, behind a new `GpuFeature` (`inline_ctes`) for one-JVM A/B.
- `GpuPlans.supports` becomes `unsupported(p)`, which names the first node or expression the GPU doesn't run; the accelerator logs it at debug when a subtree stays on the CPU.
- A new `GpuOffloadCoverageTest` plans every `spec/tpch` query with a recording accelerator and fails when a query gains a blocker outside a short allowlist (today q2's cross join and window, q17's window). It needs no CUDA device and runs on a laptop; `WVLET_GPU_TEST_DATA` points it at larger data.
- q10: fix only if the profile shows a GPU-side cause with a small fix; otherwise record it as a follow-up.

## Alternatives and Why Not?

- Turn off the CPU's shared scans when an accelerator is present: keeps the CPU fallback slower and couples the planner to the GPU module.
- Read the CPU-computed CTE result on the GPU: an upload of host rows per query, the cost the GPU path avoids by caching columns.
- Run the coverage test in CI: CI would need cudf's ~500 MB jar; `design/engine.md` lists it as the local gate after planner changes instead.
