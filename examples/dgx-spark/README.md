# TPC-H on a DGX Spark: CPU vs GPU

Baseline for the GPU executor plan in [`plans/2026-11-24-gpu-engine-executor.md`](../../plans/2026-11-24-gpu-engine-executor.md). It compares DuckDB 2.0.1 on the CPU with DuckDB 2.0.1 + [Sirius](https://github.com/sirius-db/sirius) on the GPU, over the same TPC-H Parquet files.

## Run

- On the Spark: `./run_bench.sh 10` (scale factor 10, 3 iterations, all 22 queries).
- From a laptop: `HOST=spark ./run_bench.sh 10` copies this folder to `~/wvlet-gpu-bench/scripts` and runs it there.
- Arguments: `./run_bench.sh <sf> <iterations> <queries>`, e.g. `./run_bench.sh 10 5 1,6,14`.
- Requirements: an NVIDIA driver (CUDA 12 or 13), `python3` with `venv`, io_uring enabled, and network access to install `duckdb==2.0.1` and the `sirius` extension (~700 MB, on first `LOAD`).

## What it does

- Data: `gen_tpch.py` runs DuckDB's `dbgen` once and writes `~/wvlet-gpu-bench/tpch-sf<SF>/<table>.parquet`.
- `bench_duckdb.py` runs each mode in its own process: `cpu` with `SIRIUS_DISABLE=1`, `sirius` with `LOAD sirius` and [`sirius.yaml`](sirius.yaml).
- Timing covers `execute()` only, as in Sirius's own benchmark. The first iteration is cold; the report shows it and the best hot iteration.
- After the timed runs, each query runs once more with `enable_duckdb_fallback = false`. The `on GPU` column shows whether the whole query ran on the GPU.
- `match` compares order-insensitive result digests (floats rounded to 4 places) between CPU and GPU.
- Output: `~/wvlet-gpu-bench/results/sf<SF>-<timestamp>/report.md`, plus the per-run CSVs and logs.

## Results: SF10 on one DGX Spark (2026-11-24)

GB10, driver 580.173.02 (CUDA 13), 20 Grace cores, DuckDB 2.0.1, `sirius_cuda13`, 3 iterations. All 22 queries ran fully on the GPU (`enable_duckdb_fallback = false` passed), and their results matched the CPU run.

| mode | hot total (s) | vs CPU | cold total (s) |
| --- | ---: | ---: | ---: |
| DuckDB CPU | 3.43 | 1.00x | 3.56 |
| Sirius, reading Parquet per query | 10.16 | 0.34x | 10.76 |
| Sirius, tables pinned in GPU memory (`--pin gpu`) | 0.82 | 4.16x | 1.06 |

- Without pinning, every query decodes Parquet again and Sirius is 3x slower than the CPU; even q6 takes 0.14 s against 0.05 s. A likely cause (not profiled yet): on the GB10 the GPU and CPU share the same LPDDR5X bandwidth, so the GPU has no memory-bandwidth edge on the scan, and the decode plus per-query setup dominate at SF10.
- With the tables pinned, the per-query speedup ranges from 1.6x (q1, q2, q11) to 15.8x (q13); q10, q14 and q17 are 7-12x faster.
- The first pinned run ever on a machine took 3-9 s per query, probably for CUDA's JIT compilation of kernels for sm_121; `~/.nv/ComputeCache` caches the result, and later processes start fast.

## Results: SF100 on one DGX Spark (2026-11-24)

Same setup, with [`sirius-sf100.yaml`](sirius-sf100.yaml) (70% of memory for the GPU). The data is 32 GB of Parquet; `dbgen` plus the export took 2.5 min. All 22 queries again ran fully on the GPU and matched the CPU results. Nothing spilled to disk, and memory stayed at 105 GB used (16 GB available) while Sirius ran.

| mode | hot total (s) | vs CPU | cold total (s) |
| --- | ---: | ---: | ---: |
| DuckDB CPU | 25.94 | 1.00x | 27.46 |
| Sirius, reading Parquet per query | 31.63 | 0.82x | 46.13 |
| Sirius, tables pinned in GPU memory | 5.10 | 5.08x | 20.06 |

- Pinned, the per-query speedup ranges from 1.6x (q1) and 2.7x (q16, q18) to 9-12x (q2, q5, q14, q17).
- Unpinned, the gap to the CPU narrows with scale (0.34x at SF10, 0.82x at SF100). q9, q13, q17 and q21 are already faster on the GPU.
- Pinned cold runs pay to load columns into GPU memory on first use: q3 and q9 took 7.6 s and 7.9 s, the first queries to read most of `lineitem` and `orders`.

## Wvlet engine with GPU offload (SF10)

[`wvlet-engine-gpu`](../../wvlet-engine-gpu) runs the plan subtrees of Wvlet's own engine that join or aggregate on the GPU through cudf-java, keeping scanned columns in GPU memory across queries; see [the ADR](../../adr/2026-11-24-engine-gpu-executor.md). Same Spark and data, JDK 25, best of 3 hot runs, GPU on and off in one JVM:

| engine | total (s) | vs DuckDB CPU |
| --- | ---: | ---: |
| Wvlet engine, CPU | 10.99 | 0.31x |
| DuckDB CPU | 3.43 | 1.00x |
| **Wvlet engine + GPU** | **2.63** | **1.30x** |
| DuckDB + Sirius, pinned | 0.82 | 4.18x |

- 20 of 22 queries run as one GPU subtree (scans through joins, aggregation, and sort); q13 and q22 stay on the CPU. q4, q15, and q21 are 17-19x faster than the CPU engine; q1 (DECIMAL128 arithmetic) 1.4x; q16 is 1.3x slower.
- Results match DuckDB on spec/tpch (`GpuTPCHTest`), and row counts match the CPU engine at SF10.

To run it on a Spark (JDK 17+ and this repository checked out there):

```sh
ln -s ~/wvlet-gpu-bench/tpch-sf10 target/tpch-sf10     # or let the bench generate it
./sbt "engineGpu/testOnly *GpuTPCHTest"                # conformance, with the GPU subtrees per query
WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_RUNS=3 WVLET_ENGINE_BENCH_AB=gpu_offload \
  ./sbt "engineGpu/testOnly *GpuTPCHBench"             # writes target/engine-bench/tpch-gpu-sf10-ab.tsv
```

`CUDF_CLASSIFIER` picks the cudf build (default `cuda13-arm64`; `cuda12` for x86_64 with CUDA 12). `WVLET_GPU_POOL_FRACTION` (default 0.8 of free device memory) and `WVLET_GPU_CACHE_FRACTION` (default 0.5 of the pool) size the memory pool and the column cache.

## GB10 notes

- GPU and CPU share 128 GB of memory, so `sirius.yaml` caps Sirius at half of the GPU-visible memory and 24 GiB of pinned host pools. Raise both for SF100.
- `LOAD sirius` downloads the 820 MB `sirius_cuda13` backend on first use. DuckDB's built-in HTTP client dropped that download ("Failed to read connection"), so `bench_duckdb.py` loads `httpfs` with a longer `http_timeout` first. Installing a curl-downloaded copy fails the signature check: DuckDB trusts community signatures only from the community repository.
- Check `sirius.log` for initialization errors on the first run. `SELECT * FROM sirius_status()` shows the selected backend and any fallback reason.
