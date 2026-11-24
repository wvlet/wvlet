# Columnar engine

This page owns how `wvlet-engine` plans and executes Wvlet relations, and the gates an engine change must pass. Why the engine exists and why it executes typed plans instead of SQL is in [the columnar engine ADR](../adr/2026-11-21-columnar-engine.md); its optimizer decisions are the `adr/2026-11-2*-engine-*.md` records.

## Planning path

- Every relation is planned through the runtime IR in `wvlet.lang.engine.runtime`: `RuntimeBinder` → `RuntimePlan` → `Lowering` into physical operators (`wvlet.lang.engine.exec`).
- `Planner` (`wvlet.lang.engine.plan`) keeps the shared pieces the binder calls, generic over a plan builder: correlated joins, decorrelation, join ordering, and parallelization.
- Logical passes (`PushDownFilters`, `PruneColumns`, `ReorderJoins`, `ShareSubplans`, `Normalize`, ...) run in `Optimizer` over the runtime plan. Rules with three-valued-logic subtleties have Lean models; see [formal verification](formal-verification.md).
- `WvletEngine.runtimePlan` uses a new `Planner` per call; share one `Planner` when building plans by hand in tests.

## Optimizer toggles

- `Optimization` (`plan/OptimizerOptions.scala`) names each planner optimization. Turn them off with `wvlet.engine.optimizer.disabled=<names>`, `WVLET_ENGINE_OPTIMIZER_DISABLED`, or `WvletEngine(optimizer = ...)`.
- `EngineOptimizerToggleTest` checks results with each optimization off.
- A new toggle must be listed in `OptimizerOptionsTest`, or the JS and Native CI jobs fail.
- Give every executor or planner change an `Optimization` toggle from the start, so it can be benchmarked on one build.

## Parquet footer cache

- `ParquetFooterCache` (`catalog/`) keeps parsed footers of local Parquet files, process-wide, LRU up to 256 files. Behind `parquet_footer_cache` (on by default).
- An entry is valid while the file's size and last-modified time match a stat taken before the footer read. Files without an mtime, or modified within the last 10 seconds, are not cached, as DuckDB's `parquet_metadata_cache` does for coarse mtimes.
- Scans use it when `ScanSpec.footerCache` is set (from the toggle at lowering); planning reads when the `ParquetSource` was built with `footerCache` (file paths in queries). Other callers parse as before.
- Only local paths are cached. A remote source would need a key that changes on overwrite (e.g. an ETag), since uni's `FileInfo` has none.

## Accelerators (GPU offload)

- A `PlanAccelerator` (`runtime/PlanAccelerator.scala`) is offered each runtime plan subtree top-down in `Lowering`, before CPU lowering; the first subtree it accepts runs as one operator. `WvletEngine(accelerator = ...)` sets one; the `gpu_offload` toggle turns the hook off. See [the GPU executor ADR](../adr/2026-11-24-engine-gpu-executor.md).
- `wvlet-engine-gpu` is the example accelerator (cudf-java, JVM only, not in the CLI pack or the CI aggregates). It runs subtrees that join or aggregate, keeps scanned columns on the device across queries, and runs a subtree on the CPU if the device fails at run time.
- `EnginePlanAcceleratorTest` checks the hook with a stub accelerator. On a machine with a CUDA device, `./sbt "engineGpu/testOnly *GpuTPCHTest"` checks spec/tpch against DuckDB with GPU offload, and `WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_AB=gpu_offload ./sbt "engineGpu/testOnly *GpuTPCHBench"` compares GPU on and off.

## Correctness gates

- `EngineBasicSpecTest` runs spec/basic with `PlanValidator` on.
- Engine tests check rows against DuckDB through generated SQL (`EngineConformance.compare`), with fixed rows where DuckDB evaluates a form differently (for example `sample`, `describe`).
- Shared engine tests also run on `engineJS`; Scala.js prints `-0.0` as `0`.

## Golden plans

- Physical plans of spec/tpch and spec/tpcds are golden files under `spec/engine-plans/` (TPC-DS at SF0.1, TPC-H on the tiny `spec/tpch/data`). `EnginePlanGoldenTest` fails on drift.
- After an intended planner change, regenerate them: `./sbt "engineJVM/Test/runMain wvlet.lang.engine.EnginePlanGolden"`.
- SF1 plans can differ from the golden ones for estimate-based changes; check them with `WVLET_ENGINE_BENCH_EXPLAIN=1`.

## Function coverage ratchets

Raise `minCovered` when adding support; never lower it.

- `./sbt "engineJVM/testOnly *EngineStdlibCoverageCheck"` runs every stdlib function visible to the DuckDB dialect on the engine and on DuckDB and counts matches. This is the primary measure, because the stdlib is Wvlet's engine-neutral function contract. Report: `target/engine-stdlib-coverage.txt`.
- `./sbt "engineJVM/testOnly *EngineFunctionCoverageCheck"` counts DuckDB catalog names the engine binds (`EngineFunctions`), a compatibility measure. Report: `target/engine-function-coverage.txt`.
- Add new functions under stdlib names, not by porting DuckDB names as public API; see [stdlib](stdlib.md).

## Benchmarks

- `EngineTPCHBench` and `EngineTPCDSBench` run the spec queries on the engine and on DuckDB over the same Parquet files. They are ignored unless `WVLET_ENGINE_BENCH_SF` is set. The other `WVLET_ENGINE_BENCH_*` variables are listed in the bench classes' Scaladoc and in [the benchmark guide](../website/docs/development/benchmark.md).
- A/B an engine change behind its `Optimization` toggle in one JVM: `WVLET_ENGINE_BENCH_SF=1 WVLET_ENGINE_BENCH_RUNS=5 WVLET_ENGINE_BENCH_AB=<toggle> ./sbt "engineJVM/testOnly *EngineTPCHBench *EngineTPCDSBench"`. Read the suite on/off totals first; single queries are noisy.
- A change that cannot be toggled is benchmarked against an origin/main worktree, alternating rounds between the two trees.
