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
- The engine is faster than DuckDB on 63 of the 99 TPC-DS queries (q44 at 0.45x, q95 at 0.49x) and on 10 of the 22 TPC-H queries (q15 at 0.65x, q17 at 0.73x). TPC-DS q98 and TPC-H q16 return large results, which cost DuckDB extra time in JDBC; see [How it is measured](#how-it-is-measured).
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

- **Machine**: Apple M5 Pro (6 performance and 12 efficiency cores, 48 GB), JDK 25, a shared machine; each round started at a load average of 2.9–5.6.
- **DuckDB**: version 2.0.1 through its JDBC driver, in the same JVM. Its default thread count and the engine's default parallelism are the same, all 18 cores. DuckDB caches Parquet footers across queries (`parquet_metadata_cache = true`), as the engine does, and runs with `preserve_insertion_order = false`, a guarantee the engine doesn't make either. With DuckDB's defaults, its totals are about 5–6% higher.
- **Data**: generated with DuckDB's `tpch` and `tpcds` extensions and stored as Parquet; both engines read the same files directly, with no import into either engine's own format.
- **Runs**: each query runs once on each engine to warm up, then 3 timed runs that alternate between the engines; the result is the median. The OS file cache is warm for both.
- **Results**: both engines return every row, and every value is read from the result: through JDBC for DuckDB, and as the same boxed values for the engine. DuckDB's time includes moving the result through JDBC, about 0.4 ms per query on TPC-DS and 3 ms on TPC-H against DuckDB's own profiled query time. Only the two queries with large results move much: TPC-DS q98 (15,080 rows, 77 ms against 41 ms inside DuckDB) and TPC-H q16 (27,840 rows, 79 ms against 41 ms), so read those two ratios as up to about 1.9x in the engine's favor.
- **Rounds**: the result combines 3 full-suite rounds. DuckDB's own totals varied by 2.3% across them on TPC-DS and by 8% on TPC-H.
- **Snappy**: on the JVM, the engine decodes Snappy pages with [snappy-java](https://github.com/xerial/snappy-java); the JavaScript and Native builds use a pure-Scala decoder.

### What the engine knows that DuckDB doesn't

Two things help the engine and have no counterpart in DuckDB over Parquet files. Keep them in mind when reading the ratios:

- **File statistics.** The first query that reads a file computes statistics about it for join ordering (distinct counts of string columns, samples of their values) and keeps them while the file is unchanged, much like `ANALYZE` in a database. The timed runs come after the warm-up run, so they reuse those statistics. Recomputing them for every query makes the engine about 3.0x slower on TPC-DS and 1.9x slower on TPC-H at SF10, so a single query on files the engine hasn't seen before is slower than these results show. The benchmark records each engine's first-run time next to the medians.
- **Declared keys.** The benchmark declares the TPC-H and TPC-DS primary keys, and `c_customer_id` as a unique key of the TPC-DS `customer` table, to the engine. DuckDB reads the Parquet files without constraints. Without the keys, the engine's totals are about 7% higher on TPC-DS (q25 3.1x, q39 2.3x) and 4% higher on TPC-H.

To time either effect, run the benchmark with `WVLET_ENGINE_BENCH_COLD=1` (no statistics or footers carried between runs, and no DuckDB footer cache) or `WVLET_ENGINE_BENCH_KEYS=none`.

The benchmark is opt-in and never gates CI. To run it, see `EngineTPCHBench` and `EngineTPCDSBench` under `wvlet-engine/.jvm/src/test/scala/wvlet/lang/engine/`; for example, `WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"`. Timings depend on the machine and its load, so compare numbers from one machine only.
