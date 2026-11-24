# ADR: An example GPU executor offloads whole plan subtrees through cudf

Date: 2026-11-24

## Context

NVIDIA's Sirius runs DuckDB plans on the GPU. On one DGX Spark (GB10, 128 GB of memory shared by CPU and GPU, CUDA 13), DuckDB + Sirius ran TPC-H SF10 4.2x and SF100 5.1x faster than DuckDB on the CPU, but only with the tables pinned in GPU memory; reading Parquet on every query made it slower than the CPU (0.34x at SF10). See [`examples/dgx-spark/`](../examples/dgx-spark/README.md). The question was whether Wvlet's own engine can use the GPU the same way, as a demonstration, without changing how the CPU engine plans or runs.

## Decision

- The engine gets a `PlanAccelerator` hook (`runtime/PlanAccelerator.scala`). `Lowering` offers each runtime plan subtree to it top-down before lowering on the CPU; the first subtree it accepts becomes one operator, and its inputs are not offered again. So an accelerator runs the largest subtrees it fully supports, and the plan above them stays on the CPU. The `gpu_offload` toggle disables the hook, so GPU on/off is an A/B run on one build.
- The accelerator also receives a CPU-only lowering of the subtree. The GPU operator runs it when the device fails at run time (e.g. out of memory), so a query never fails because of the GPU.
- `wvlet-engine-gpu` implements the hook with cudf-java (`ai.rapids:cudf`). cudf already has Parquet reading, filters, group-by, hash joins (with gather maps), and sort on the device, so the module translates the engine's `RuntimePlan` and `VExpr` nodes rather than writing CUDA kernels. Expressions without the CPU engine's exact semantics on cudf (decimal division, rounding casts, case-insensitive LIKE, most row functions) are not offloaded.
- Scanned columns stay on the device in an LRU cache (`GpuColumnCache`), keyed by file, column, size, and modification time. This plays the role of Sirius's `pin_table` without an explicit command: the second query that reads a column skips Parquet decoding.
- Only subtrees that join or aggregate are offloaded. A bare scan or filter returns about as many rows as it reads, and copying them back to the host cost more than the CPU's own scan (TPC-H q13 and q22 went from 0.5 s to 1.9 s and 0.43 s at SF10).
- The module is JVM-only and optional: it is not in the CLI pack or the CI aggregates (the cudf jar is 300-700 MB, per CUDA version and CPU), and nothing outside it references cudf. Its tests skip themselves without a CUDA device.

## Consequences

- TPC-H SF10 on a DGX Spark (hot runs, columns cached): 2.63 s with GPU offload against 10.99 s on the CPU engine (4.2x), and 3.43 s for DuckDB on the CPU; DuckDB + Sirius with pinned tables takes 0.82 s. 20 of the 22 queries run as one GPU subtree; results match DuckDB on spec/tpch.
- Results are copied to the host once per GPU subtree, as one batch. Plans whose GPU subtree outputs many rows, or that need CPU-only nodes in the middle (window functions, outer joins with residuals, CTE reads), gain little.
- The cache and the RMM pool share device memory: SF100 needs more than one GB10's pool for the whole suite and will evict. Spilling, chunked scans, and multi-GPU execution are not part of this example.
- The engine's CPU paths are unchanged: without an accelerator, `Lowering` calls the same CPU lowering as before.
