---
sidebar_label: Performance
description: "The Wvlet engine on TPC-H and TPC-DS at scale factor 10 against DuckDB, with DataFusion and Trino as references: the results, the setup, and how to reproduce them."
---

# Performance

The [Wvlet engine](wvlet-engine.md) runs every TPC-H and TPC-DS query at scale factor 10. This page reports its speed on those suites against DuckDB, with DataFusion and Trino as references, and how to reproduce the numbers.

## Results

At scale factor 10, the Wvlet engine is on par with DuckDB: 0.90x of DuckDB's time on TPC-DS and 1.04x on TPC-H, by geometric mean. The numbers are steady-state, with a warm JVM and file cache; see [What the engine knows that DuckDB doesn't](#what-the-engine-knows-that-duckdb-doesnt) for what that includes.

TPC-H and TPC-DS at scale factor 10 (about 10 GB of raw data as Parquet files), commit [`62fdfcd4`](https://github.com/wvlet/wvlet/commit/62fdfcd4), 2026-11-27. Each number is the engine's time divided by DuckDB's on the same queries: lower is better, and 1.00x is DuckDB's time.

| Suite | Wvlet engine, total | Wvlet engine, geometric mean | Total time, Wvlet / DuckDB |
| --- | ---: | ---: | ---: |
| TPC-H (22 queries) | 1.02x | 1.04x | 2.12 s / 2.07 s |
| TPC-DS (99 queries) | 0.91x | 0.90x | 6.11 s / 6.73 s |

- Every query ran with no errors.
- The engine is faster than DuckDB on 63 of the 99 TPC-DS queries (q44 at 0.45x, q95 at 0.49x) and on 10 of the 22 TPC-H queries (q15 at 0.65x, q17 at 0.73x).
- The largest ratios are TPC-DS q01 (2.82x) and TPC-H q1 (1.56x), each suite's first query, which still carries JIT warm-up; next are TPC-DS q02 (1.79x) and TPC-H q2 (1.43x).

## Other engines as references

The same queries (as SQL for the other engines), on the same SF10 Parquet data and machine. Each engine is divided by DuckDB in its own run, because they were measured on different days:

| Engine | TPC-H, total / geometric mean | TPC-DS, total / geometric mean |
| --- | ---: | ---: |
| Wvlet engine | 1.02x / 1.04x | 0.91x / 0.90x |
| DataFusion 55.0.0 (Python bindings, 2026-11-25) | 1.90x / 1.67x | 3.59x / 1.73x |
| Trino 484 (single node, 2026-11-24) | 18.2x / 13.4x | 21.9x / 18.6x (89 queries) |

- DataFusion and Trino were measured against DuckDB's default settings, which are about 5–6% slower than the DuckDB setup in [How it is measured](#how-it-is-measured).
- DataFusion's TPC-DS total is dominated by one query, q72; its geometric mean is the fairer comparison.
- Trino ran single-node in its in-process testing harness with default settings and a 16 GB query memory limit, not as a tuned cluster, and rejected 10 TPC-DS queries written in DuckDB's SQL. Trino is built for clusters, so treat it only as a rough reference.

## How it is measured

- **Machine**: Apple M5 Pro (18 cores, 48 GB), JDK 25.
- **DuckDB**: version 2.0.1 through JDBC in the same JVM, on all 18 cores, with its Parquet footer cache on and `preserve_insertion_order = false` (about 5–6% faster than its defaults).
- **Data**: Parquet files generated with DuckDB's `tpch` and `tpcds` extensions; both engines read the same files directly.
- **Runs**: one warm-up run, then the median of 3 timed runs that alternate between the engines, over 3 full-suite rounds with a warm OS file cache.
- **Results**: both engines return and read every row. TPC-DS q98 and TPC-H q16 return large results, and moving them through JDBC adds up to about 1.9x to DuckDB's time.

### What the engine knows that DuckDB doesn't

- **File statistics**: the engine keeps per-file statistics for join ordering, much like `ANALYZE`, and the timed runs reuse them. A first query on files the engine hasn't seen is about 2–3x slower.
- **Declared keys**: the benchmark declares the TPC primary keys to the engine; DuckDB reads the files without constraints. Without the keys, the engine is 4–7% slower.

## Reproduce

The benchmark is opt-in and never gates CI: `WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"` (or `*EngineTPCDSBench`). Set `WVLET_ENGINE_BENCH_COLD=1` to drop the statistics between runs, or `WVLET_ENGINE_BENCH_KEYS=none` to drop the declared keys. Compare numbers from one machine only.
