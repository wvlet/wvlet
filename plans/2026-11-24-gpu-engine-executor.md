# GPU executor example for the Wvlet engine (DGX Spark TPC-H demo)

## Goal

- Run TPC-H on Wvlet's own engine with plan fragments offloaded to an NVIDIA GPU, as a demo on a DGX Spark (GB10, aarch64, unified memory, CUDA 13).
- Compare four configurations on the same Parquet files: Wvlet engine CPU, Wvlet engine + GPU, DuckDB 2.0.1 CPU (`SIRIUS_DISABLE=1`), and DuckDB 2.0.1 + Sirius (`INSTALL sirius; LOAD sirius;`).
- This is an example, not a production executor. Unsupported plan nodes stay on the CPU engine.

## Design

- **GPU library:** cudf-java (`ai.rapids:cudf:26.10.2`, classifier `cuda13-arm64` on the Spark). It provides Parquet reading, filters, projections, group-by, hash joins, and sort on device, so the example does not need its own CUDA kernels.
- **Engine hook (cross-platform, in `wvlet-engine`):** a `PlanAccelerator` trait. `Lowering` offers it each `RuntimePlan` subtree top-down and takes back an `Operator`, or `None` to lower the subtree on the CPU. `WvletEngine(accelerator = ...)` defaults to `None`. A new `Optimization.GpuOffload` toggle gates it (listed in `OptimizerOptionsTest`), so A/B runs need only one build.
- **Module `wvlet-engine-gpu` (JVM only, optional):**
  - `GpuAccelerator` claims maximal supported subtrees: `Scan` of Parquet (with column projection), `Filter`/`Project` over a supported `VExpr` subset (arithmetic, comparisons, and/or/not, literals, casts, dates, decimals, simple `LIKE`), `Aggregate` (sum/count/avg/min/max, count distinct), `Join` (inner/left/semi/anti hash), `Sort` with an optional limit.
  - Each claimed subtree runs on the device. The operator copies its result to a host `Batch` once, at the boundary.
  - Memory: an RMM pool sized as a fraction of the GB10's unified memory.
  - Registered through ServiceLoader, like connectors, so runner/CLI code never references cudf. A profile enables it with `type: wvlet` and `properties: { accelerator: gpu }`.
  - Its tests skip when no CUDA device is present, so CI on macOS and x86 only compiles the module.
- **Demo:** `examples/dgx-spark/` holds the following:
  - A script that builds `wv` on the Spark.
  - Data generation at SF10/SF100 through DuckDB `tpch` with `COPY ... (FORMAT PARQUET)`.
  - The run itself: 22 queries × 4 configs, cold plus hot runs. It writes `runtimes.csv` and a Markdown table.

## Status (2026-11-24)

- Done: the hook, the toggle, and `wvlet-engine-gpu` with scans (cached), filters, projections, aggregations, inner/left/semi/anti joins, sorts, limits, and DISTINCT. TPC-H SF10: 2.63 s vs 10.99 s on the CPU engine and 3.43 s on DuckDB; see [`examples/dgx-spark/`](../examples/dgx-spark/README.md) and [the ADR](../adr/2026-11-24-engine-gpu-executor.md).
- Changed from the design: no ServiceLoader or profile property yet (the bench and tests construct `GpuAccelerator` directly), and only subtrees that join or aggregate are offloaded.
- Follow-up: [`2026-11-24-gpu-executor-speedup.md`](2026-11-24-gpu-executor-speedup.md) took SF10 to 0.74-0.79 s (DuckDB + Sirius pinned: 0.82 s), with all 22 queries on the GPU.
- Next: a `wv` / profile switch (`accelerator: gpu`), SF100 with cache eviction, sorted group-by keys (q18), and splitting work across both Sparks.

## Phases

1. Hook, toggle, module skeleton, Scan/Filter/Project/Aggregate (TPC-H q1, q6), and the 4-way bench script with Sirius. Verify that cudf loads on the GB10 (sm_121).
2. Joins and sort, to cover the remaining TPC-H queries. Add a per-query report of the GPU vs CPU node split (EXPLAIN marks `Gpu*` operators).
3. (Optional) Use both Sparks by splitting the partitioned scans over the ConnectX-7 link. Not in scope for the first PR.

## Risks

- Whether cudf-java's CUDA 13 build includes sm_121 kernels, and how RMM behaves on GB10 unified memory. Phase 1 checks both first on the Spark.
- Host↔device copies at each CPU/GPU boundary can erase the gains for small scale factors. The demo uses SF ≥ 10.
- Decimal semantics: cudf decimal128 scale rules must match the engine. `EngineConformance` checks the GPU results against DuckDB.
