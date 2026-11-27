---
sidebar_label: Performance
description: "The Wvlet engine on TPC-H and TPC-DS at scale factor 10 against DuckDB, with DataFusion and Trino as references: the results, the setup, and how to reproduce them."
---

# Performance

The [Wvlet engine](wvlet-engine.md) runs every TPC-H and TPC-DS query at scale factor 10, and its conformance tests check every result row against DuckDB's. This page reports its speed on those suites against DuckDB, with DataFusion and Trino as references, and how to reproduce the numbers.

## Results

TPC-H and TPC-DS at scale factor 10 (about 10 GB of raw data as Parquet files), commit [`4bee5ae1`](https://github.com/wvlet/wvlet/commit/4bee5ae1), 2026-11-27. Each number is the engine's time divided by DuckDB's on the same queries: lower is better, and 1.00x is DuckDB's time.

| Suite | Wvlet engine, total | Wvlet engine, geometric mean | Total time, Wvlet / DuckDB |
| --- | ---: | ---: | ---: |
| TPC-H (22 queries) | 1.41x | 1.39x | 3.12 s / 2.21 s |
| TPC-DS (99 queries) | 1.20x | 1.11x | 8.82 s / 7.38 s |

- Every query ran with no errors and returned the same number of rows as DuckDB. The TPC-H and TPC-DS conformance tests compare the rows themselves, on smaller data (TPC-H tiny, TPC-DS SF0.1).
- The engine is faster than DuckDB on 31 of the 99 TPC-DS queries (q32 at 0.41x, q15 at 0.56x) and on 3 of the 22 TPC-H queries (q15 and q16 at 0.85x).
- The largest ratios are TPC-DS q27 (2.63x) and TPC-H q5 (2.23x); most others are within 2x.

## Other engines as references

The same queries (as SQL for the other engines), on the same SF10 Parquet data and machine. Each engine is divided by DuckDB in its own run, because they were measured on different days:

| Engine | TPC-H, total / geometric mean | TPC-DS, total / geometric mean |
| --- | ---: | ---: |
| Wvlet engine | 1.41x / 1.39x | 1.20x / 1.11x |
| DataFusion 55.0.0 (Python bindings, 2026-11-25) | 1.90x / 1.67x | 3.59x / 1.73x |
| Trino 484 (single node, 2026-11-24) | 18.2x / 13.4x | 21.9x / 18.6x (89 queries) |

- DataFusion's TPC-DS total is dominated by one query, q72; its geometric mean is the fairer comparison.
- Trino ran single-node in its in-process testing harness with default settings and a 16 GB query memory limit, not as a tuned cluster, and rejected 10 TPC-DS queries written in DuckDB's SQL. Trino is built for clusters, so treat it only as a rough reference.

## How it is measured

- **Machine**: Apple M5 Pro (18 cores, 48 GB), JDK 25, a shared machine with a load average of about 5.
- **DuckDB**: version 2.0.1 through its JDBC driver, in the same JVM, with its default thread count.
- **Data**: generated with DuckDB's `tpch` and `tpcds` extensions and stored as Parquet; both engines read the same files directly.
- **Runs**: each query runs once to warm up, then 3 timed runs; the result is the median.
- **Declared keys**: the benchmark declares the TPC-H and TPC-DS primary keys, and `c_customer_id` as a unique key of the TPC-DS `customer` table, to the Wvlet engine; DuckDB reads the Parquet files without constraints.
- **Snappy**: at this commit the engine decoded Snappy pages with its pure-Scala decoder; later JVM builds use snappy-java.

The benchmark is opt-in and never gates CI. To run it, see `EngineTPCHBench` and `EngineTPCDSBench` under `wvlet-engine/.jvm/src/test/scala/wvlet/lang/engine/`; for example, `WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"`. Timings depend on the machine and its load, so compare numbers from one machine only.
