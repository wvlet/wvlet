---
sidebar_label: GPU Executor (experimental)
description: "An experimental example module runs the Wvlet engine's joins and aggregations on an NVIDIA GPU through cudf, with results on TPC-H SF10 on one DGX Spark."
---

# GPU Executor (experimental)

:::warning Experimental example
The GPU executor is an example module, `wvlet-engine-gpu`, that you build and run from the repository with sbt. It is not part of the CLI, the release packages, or CI, and its tests skip themselves without a CUDA device.
:::

The [Wvlet engine](wvlet-engine.md) can hand whole plan subtrees to an accelerator before it plans them for the CPU. The GPU module implements that hook with [cudf-java](https://github.com/rapidsai/cudf), so it translates the engine's plans instead of writing CUDA kernels. See the design in [ADR: GPU executor](https://github.com/wvlet/wvlet/blob/main/adr/2026-11-24-engine-gpu-executor.md) and its follow-ups on [runtime choices](https://github.com/wvlet/wvlet/blob/main/adr/2026-11-24-gpu-executor-runtime-choices.md), [column properties](https://github.com/wvlet/wvlet/blob/main/adr/2026-11-24-gpu-column-properties.md), and [chunked scans](https://github.com/wvlet/wvlet/blob/main/adr/2026-11-24-gpu-chunked-scans.md).

## How it works

- The engine offers each plan subtree to the GPU top-down; the largest subtree the GPU fully supports runs there as one operator, and the plan above it stays on the CPU.
- Only subtrees that join or aggregate are offloaded. A bare scan or filter returns about as many rows as it reads, and copying them back costs more than scanning on the CPU.
- Scanned columns stay in GPU memory in an LRU cache, so a second query that reads a column skips Parquet decoding.
- Expressions whose results on cudf would differ from the CPU engine's (decimal division, rounding casts, case-insensitive `LIKE`, most row functions) stay on the CPU.
- If the device fails at run time, for example out of memory, the subtree runs on the CPU instead, so a query never fails because of the GPU.

## Results

TPC-H at scale factor 10 on one NVIDIA DGX Spark (GB10, 128 GB of memory shared by CPU and GPU, CUDA 13), JDK 25, best of 3 hot runs with columns cached, 2026-11-24. The GPU and CPU engine numbers come from the same JVM:

| Engine | Total (s) | vs DuckDB on the CPU |
| --- | ---: | ---: |
| Wvlet engine, CPU | 7.98 | 0.43x |
| DuckDB, CPU | 3.43 | 1.00x |
| Wvlet engine + GPU | 0.66-0.71 | 4.8-5.2x |
| DuckDB + [Sirius](https://github.com/sirius-db/sirius), tables pinned in GPU memory | 0.82 | 4.18x |

- All 22 queries run as one GPU subtree, and results match DuckDB on the TPC-H conformance queries.
- These are hot runs: the first query that reads a column pays the Parquet decode and the copy to the device.
- See [examples/dgx-spark](https://github.com/wvlet/wvlet/tree/main/examples/dgx-spark) for per-query times, SF100 results, and how each optimization changed them.

## Limits

- JVM only. The cudf jar is 300-700 MB per CUDA version and CPU, which is why the module stays out of the CLI.
- Results come back to the host once per GPU subtree, as one batch. Plans whose GPU subtree outputs many rows, or that need CPU-only operators in the middle (window functions, outer joins with residuals, CTE reads), gain little.
- The column cache and the memory pool share device memory. Spilling and multi-GPU execution are not supported.
- Tested on TPC-H only.

## Running it

On a machine with an NVIDIA GPU and CUDA, with JDK 17+ and the repository checked out:

```sh
./sbt "engineGpu/testOnly *GpuTPCHTest"     # conformance against DuckDB, printing the GPU subtrees per query
WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_RUNS=3 WVLET_ENGINE_BENCH_AB=gpu_offload \
  ./sbt "engineGpu/testOnly *GpuTPCHBench"  # GPU on and off in one JVM
```

`CUDF_CLASSIFIER` picks the cudf build (default `cuda13-arm64`; `cuda12` for x86_64 with CUDA 12). The [example README](https://github.com/wvlet/wvlet/tree/main/examples/dgx-spark) lists the memory and scan settings.
