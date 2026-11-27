---
sidebar_label: Performance
description: "How fast the Wvlet engine runs TPC-H and TPC-DS at scale factor 10 compared with DuckDB, with DataFusion and Trino as references: the setup, the totals, and where it is still slower."
---

# Performance

The [Wvlet engine](wvlet-engine.md) is younger than the engines it is compared with here, and it is still slower than DuckDB overall. This page reports where it stands, measured on the TPC-H and TPC-DS benchmark queries.

## Results

TPC-H and TPC-DS at scale factor 10 (about 10 GB of raw data, stored as Parquet files), measured on 2026-11-27 at commit [`4bee5ae1`](https://github.com/wvlet/wvlet/commit/4bee5ae1) on an Apple M5 Pro (18 cores, 48 GB) with JDK 25, a shared machine with a load average of about 5. Each number is the engine's time divided by DuckDB's on the same queries, so lower is better and 1.00x means as fast as DuckDB:

| Suite | Wvlet engine, total | Wvlet engine, geometric mean | Total time, Wvlet / DuckDB |
| --- | ---: | ---: | ---: |
| TPC-H (22 queries) | 1.41x | 1.39x | 3.12 s / 2.21 s |
| TPC-DS (99 queries) | 1.20x | 1.11x | 8.82 s / 7.38 s |

- Every query ran with no errors, and the engine's results match DuckDB's: the TPC-H and TPC-DS conformance tests compare their rows.
- The engine is faster than DuckDB on 31 of the 99 TPC-DS queries (q32 at 0.41x, q15 at 0.56x) and on 3 of the 22 TPC-H queries (q15 and q16 at 0.85x).
- The slowest are TPC-DS q27 (2.63x) and TPC-H q5 (2.23x); most others are within 2x.

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

- Both engines run in one JVM and read the same Parquet files: DuckDB through its JDBC driver (2.0.1) with its default thread count.
- Each query runs once to warm up, then 3 timed runs; the result is the median.
- The data is generated with DuckDB's `tpch` and `tpcds` extensions.
- The benchmark is opt-in and never gates CI. To run it on your machine, see `EngineTPCHBench` and `EngineTPCDSBench` under `wvlet-engine/.jvm/src/test/scala/wvlet/lang/engine/`; for example, `WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"`.

Timings depend on the machine and its load, so compare numbers from one machine only.
