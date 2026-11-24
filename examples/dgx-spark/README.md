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

[`wvlet-engine-gpu`](../../wvlet-engine-gpu) runs the plan subtrees of Wvlet's own engine that join or aggregate on the GPU through cudf-java, keeping scanned columns in GPU memory across queries; see [the ADR](../../adr/2026-11-24-engine-gpu-executor.md) and its follow-ups on [runtime choices](../../adr/2026-11-24-gpu-executor-runtime-choices.md) and [column properties](../../adr/2026-11-24-gpu-column-properties.md). Same Spark and data, JDK 25, best of 3 hot runs, GPU on and off in one JVM:

| engine | total (s) | vs DuckDB CPU |
| --- | ---: | ---: |
| Wvlet engine, CPU | 7.98 | 0.43x |
| DuckDB CPU | 3.43 | 1.00x |
| Wvlet engine + GPU, first version (#2363) | 2.63 | 1.30x |
| Wvlet engine + GPU, #2366 | 0.74-0.79 | 4.3-4.6x |
| **Wvlet engine + GPU** | **0.66-0.71** | **4.8-5.2x** |
| DuckDB + Sirius, pinned | 0.82 | 4.18x |

- All 22 queries run as one GPU subtree. Against Sirius pinned, the Wvlet engine is faster on 14 queries (e.g. q18 45 vs 78 ms, q8 16 vs 45 ms, q21 44 vs 75 ms, q6 12 vs 14 ms) and within 2 ms on q1 (67-70 vs 66 ms), q15, and q17. It is slower on q5, q12, and q19 by 1-8 ms each; q11 varies between 19 and 37 ms across runs (Sirius 24 ms). The largest gap left is q13 (75 vs 18 ms: a left join with a residual, where the LIKE over 15M comments alone takes 24 ms).
- Since #2366, q1 went from 127 to 67-70 ms. The filter that keeps 59M of 60M rows became a group key instead of a copy, `avg` became an exact sum and count, and its string keys group on dictionary codes. q18 went from 65 to 45 ms with a sorted group-by on `l_orderkey`, and q6 and q12 dropped 4-7 ms each from filter comparisons fused into one cudf AST kernel. The CPU engine's 7.98 s is from the same JVM as this GPU run.
- The gains over the first version came from DECIMAL64 arithmetic and sums chosen by the data's value bounds, filtering join inputs by the other side's keys, predicates implied by OR filters, pruning unread join columns, and bringing q13, q16, and q22 onto the GPU. The CPU engine's 7.56 s comes from the same JVM as the GPU run and includes later CPU engine changes (#2364, #2365); the earlier 10.99 s came from a separate run.
- Results match DuckDB on spec/tpch and at SF10 (`WVLET_GPU_TEST_DATA=target/tpch-sf10 ./sbt "engineGpu/testOnly *GpuTPCHTest"`).
- Each optimization is a `GpuFeature`: `WVLET_GPU_DISABLE=<name>,...` turns features off, and `WVLET_GPU_BENCH_AB=<name>,...` compares them on and off in one JVM. `WVLET_GPU_PROFILE=1` logs the device time of each plan node.

### SF100 (2026-11-24)

The same bench at SF100, GPU on and off in one JVM, best of 2 hot runs:

```sh
WVLET_GPU_TEST_HEAP=28g WVLET_ENGINE_BENCH_SF=100 WVLET_ENGINE_BENCH_RUNS=2 \
  WVLET_GPU_POOL_FRACTION=0.7 WVLET_GPU_CACHE_FRACTION=0.6 WVLET_ENGINE_BENCH_TIMEOUT=900 \
  WVLET_ENGINE_BENCH_AB=gpu_offload ./sbt "engineGpu/testOnly *GpuTPCHBench"
```

- All 22 queries run on the GPU, with row counts matching the CPU engine and no CPU fallback: 9.17 s against 99.3 s on the CPU engine (10.8x), and 5.10 s for DuckDB + Sirius with pinned tables (DuckDB on the CPU: 25.94 s).
- Faster than Sirius pinned on q2, q18, q20, and q22, and within 10% on q21. The largest gaps are q1 (1.41 vs 0.58 s), q13 (1.18 vs 0.35 s), q19 (0.74 vs 0.13 s), and q12 (0.65 vs 0.14 s): they stream lineitem or orders through the GPU in 32M-row ranges, because their columns don't fit in the pool whole or their strings exceed cudf's 2 GB column limit. See [the chunked scans ADR](../../adr/2026-11-24-gpu-chunked-scans.md).
- Before chunked scans, q1, q12, q13, and q19 failed on the device and then ran on the CPU (17-37 s each), and q7, q18, and q19 ran out of the CPU engine's 16 GB heap. `WVLET_GPU_TEST_HEAP=28g` sets the test JVM's heap; with the pool at 0.7 of free memory (74 GiB), both fit in the GB10's 128 GB.

| query | Wvlet engine + GPU (ms) | Wvlet engine, CPU (ms) | DuckDB + Sirius, pinned (ms) |
| --- | ---: | ---: | ---: |
| q1 | 1,412 | 7,111 | 580 |
| q2 | 42 | 603 | 43 |
| q3 | 293 | 2,680 | 183 |
| q4 | 138 | 1,886 | 107 |
| q5 | 368 | 3,650 | 145 |
| q6 | 189 | 1,570 | 58 |
| q7 | 271 | 11,964 | 192 |
| q8 | 353 | 3,850 | 198 |
| q9 | 761 | 9,192 | 670 |
| q10 | 357 | 3,718 | 187 |
| q11 | 204 | 516 | 68 |
| q12 | 646 | 1,996 | 143 |
| q13 | 1,178 | 4,427 | 351 |
| q14 | 112 | 1,967 | 91 |
| q15 | 223 | 4,147 | 125 |
| q16 | 316 | 838 | 143 |
| q17 | 174 | 3,255 | 109 |
| q18 | 630 | 20,355 | 824 |
| q19 | 744 | 3,816 | 131 |
| q20 | 121 | 4,042 | 128 |
| q21 | 534 | 6,449 | 500 |
| q22 | 106 | 1,265 | 127 |
| **total** | **9.17 s** | 99.3 s | 5.10 s |

To run it on a Spark (JDK 17+ and this repository checked out there):

```sh
ln -s ~/wvlet-gpu-bench/tpch-sf10 target/tpch-sf10     # or let the bench generate it
./sbt "engineGpu/testOnly *GpuTPCHTest"                # conformance, with the GPU subtrees per query
WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_RUNS=3 WVLET_ENGINE_BENCH_AB=gpu_offload \
  ./sbt "engineGpu/testOnly *GpuTPCHBench"             # writes target/engine-bench/tpch-gpu-sf10-ab.tsv
```

`CUDF_CLASSIFIER` picks the cudf build (default `cuda13-arm64`; `cuda12` for x86_64 with CUDA 12). `WVLET_GPU_POOL_FRACTION` (default 0.8 of free device memory) and `WVLET_GPU_CACHE_FRACTION` (default 0.5 of the pool) size the memory pool and the column cache. `WVLET_GPU_STREAM_ROWS` (default 100M), `WVLET_GPU_STREAM_FRACTION` (default 0.9), and `WVLET_GPU_CHUNK_ROWS` (default 32M) decide which scans stream and how many rows each range has.

## GB10 notes

- GPU and CPU share 128 GB of memory, so `sirius.yaml` caps Sirius at half of the GPU-visible memory and 24 GiB of pinned host pools. Raise both for SF100.
- `LOAD sirius` downloads the 820 MB `sirius_cuda13` backend on first use. DuckDB's built-in HTTP client dropped that download ("Failed to read connection"), so `bench_duckdb.py` loads `httpfs` with a longer `http_timeout` first. Installing a curl-downloaded copy fails the signature check: DuckDB trusts community signatures only from the community repository.
- Check `sirius.log` for initialization errors on the first run. `SELECT * FROM sirius_status()` shows the selected backend and any fallback reason.
