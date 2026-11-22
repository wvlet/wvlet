# Wvlet columnar query engine (`wvlet-engine`)

Date: 2026-11-21

## Goal

A cross-platform (JVM / Scala.js / Scala Native) vectorized query engine that executes Wvlet
logical plans directly — no SQL round trip, no JDBC, no parquet-java/Hadoop — reading Parquet,
Parquet datasets and Iceberg tables through uni 2026.3.0's pure-Scala readers. The target is
DuckDB-comparable coverage of analytical queries: every spec/basic query that does not need an
external engine, all 22 TPC-H queries with results identical to DuckDB, and then TPC-DS.

## Why now

uni 2026.3.0 ships a cross-platform Parquet reader/writer (row-group pruning, column
projection, row filters, dataset partitions) and Iceberg / Delta table readers and writers.
The storage layer — the part that used to force a JVM-only parquet-java + Hadoop stack — is
done, so what remains is the execution layer, which is plain Scala.

## Design

### Where it plugs in

- New module `wvlet-engine` (crossProject JVM/JS/Native) depending on `wvlet-lang` and `uni`
- Input: the typed, rewritten `Relation` that `BasePlanExecutor.executeQuery` already receives
  (the same tree `GenSQL` turns into SQL). SQL input works too, since `SqlParser` produces the
  same plan nodes
- Output: `QueryResult` (`TableRows`) built from the result batches, so `test` assertions,
  printers and the REPL work unchanged
- Selection: a connector type `wvlet` (the in-process engine). `PlanExecutor` /
  `QueryExecutor` route `executeQuery` to the engine when the active connector is `wvlet`,
  and fall back to SQL engines otherwise

### Execution model

- Pull-based, vectorized: each `Operator` produces `Batch`es (column vectors of up to 4096
  rows, selection vector optional) via `next(): Batch | Null`
- `Vector` types: Bool, Int32, Int64, Float64, Decimal (unscaled Long for precision <= 18,
  BigDecimal otherwise), String (UTF-8 offsets + bytes, materialized lazily), Date (Int32
  days), Timestamp (Int64 micros), and a boxed `AnyVector` for lists/maps/structs. Nulls are
  a validity bitmap
- Expressions compile once per operator into `VectorExpr` closures (no per-row tree walk);
  type-specialized kernels for arithmetic, comparison, boolean logic, CASE, IN, LIKE, CAST
- Physical operators: Scan (Values, Parquet file / dataset / Iceberg, JSON, CSV), Filter,
  Project, HashAggregate (grouping + global), Sort, TopN, Limit/Offset, HashJoin (inner,
  left, right, full, semi, anti, mark for EXISTS/IN subqueries), NestedLoopJoin (non-equi,
  cross), Union/Concat/Intersect/Except, Distinct, Window, Unnest, Pivot/Unpivot, Sample
- Planner: `Relation` → physical tree; projection and filter pushdown into the Parquet scan
  (`ParquetReadOptions.withColumns` / `withFilter`), constant folding reused from the
  compiler, correlated subqueries decorrelated into joins
- Parallelism (JVM / Native): morsel-driven over row groups for scans, partitioned hash
  aggregation and join build; single-threaded on Scala.js

### Semantics

- Wvlet semantics first (string `+` treats NULL as empty, see ADR 2026-11-18), SQL three-
  valued logic otherwise; decimal and integer arithmetic follow `TyperRules`
- Conformance: a `RunnerSpecEngine*` suite runs the same `.wv` specs on DuckDB and on the
  engine and compares result tables (types normalized), so DuckDB is the oracle

## Status

- Milestones 2–3 core landed together: TPC-H 22/22 matches DuckDB; spec/basic 115/162

## Performance tracking

`EngineTPCHBench` (see website/docs/development/benchmark.md) compares the engine with DuckDB on
spec/tpch at a given scale factor. Baseline on an 18-core Apple Silicon laptop, sf=1, DuckDB
with its default thread count, sum of the per-query medians: DuckDB 0.36 s, engine 42.3 s
(118x). Plan, roughly one PR each:

1. Benchmark harness (this baseline); fix sorted output of more than 4096 rows (q16 at sf=1)
2. Column pruning and Parquet row-group pruning by statistics
3. Faster kernels: scalar comparisons, typed hash tables for grouping and joins, selection
   vectors, dictionary-encoded strings, incremental window aggregates
4. Parallelism on JVM / Native: morsel-driven scans over row groups, partitioned hash
   aggregation and join build (Scala.js stays single-threaded)

Progress (sf=1, sum of per-query medians over q1–q22 without q16, DuckDB ≈ 0.32 s):

| Step | Engine | vs DuckDB |
|---|---:|---:|
| Baseline (#2200) | 42.3 s | 118x |
| Column pruning, row-group pruning (#2204) | 10.8 s | 31x |
| Kernels, typed hash tables (#2205) | 8.6 s | 26x |
| Parallel scans, join probes, two-phase aggregation (#2210) | 2.1 s | 6.4x |
| Parallel right / full joins, outer joins build on the smaller side (#2214; all 22 queries) | 2.0 s | 5.2x |
| Partitioned join builds and grouped aggregation, swapped semi / anti joins (#2218) | 1.6 s | 4.4x |
| Parallel sorts, top-N, and partitioned windows (#2219; no TPC-H sort reaches 64K rows) | unchanged | |
| Column pruning through windows, unboxed window outputs, parallel row partitioning (no TPC-H windows) | unchanged | |
| Split sort merges into parallel output ranges, bounded-heap top-N (no TPC-H sort reaches 64K rows) | unchanged | |
| Typed window aggregate / value outputs, unboxed partition sort, reused partition hashes (no TPC-H windows) | unchanged | |
| Incremental running-frame window aggregates (no TPC-H windows) | unchanged | |
| Sliding-frame counts and exact sums with add/remove (no TPC-H windows) | unchanged | |

Parallel sorts and windows (A/B with the parallel paths disabled, sf=1 lineitem, 6M rows):
top-N (limit 10) 2.29 s → 0.15 s, full sort 2.3 s → 1.1–1.3 s (bounded by the merge on one
thread). A window partitioned by l_orderkey stayed in the noise (0.6–1.2 s vs 0.9–1.1 s). Profiling
showed that its "materialize" step was really the scan: column pruning stopped below windows,
so the window read all 16 lineitem columns. With pruning through windows, rank / row_number
written to primitive arrays, a counting sort for partition rows, and parallel row
partitioning (A/B against main, interleaved): `rank() over (partition by l_orderkey order by
l_extendedprice)` 0.62–0.98 s → 0.13–0.14 s; row_number and sum over two other PARTITION BYs
0.65–0.97 s → 0.31–0.33 s.
Splitting the merge into one output range per thread (splitter search over the sorted runs) and
a bounded heap per chunk for small limits (A/B against main, interleaved, same output): full sort
1.19–1.39 s → 0.25–0.41 s; top-N limit 10 0.15–0.23 s → 0.02–0.04 s, limit 1000 0.16–0.22 s →
0.02–0.06 s.
With aggregates and value functions also written to typed columns, partitions sorted without
boxing, and first / last / nth_value scanning frames without building collections (A/B against
main, interleaved, 3 rounds of 5 runs, medians): lag + first_value over two PARTITION BYs
1.2–1.9 s → 0.29–0.36 s; row_number and sum 0.28–0.41 s → 0.22–0.30 s; count and sum over
two PARTITION BYs 0.36–0.45 s → 0.21–0.27 s; rank unchanged (0.13–0.27 s both). Running frames
still aggregate each frame from scratch: `sum(l_quantity) over (partition by l_suppkey order by
l_orderkey)` (about 600 rows per partition) takes 0.7 s against 0.26 s with about 30 rows per
partition, so incremental running aggregates are the next window step.
Running frames now fold one accumulator per partition, feeding each row only the rows added to
its frame (A/B against main, interleaved, 3 rounds of 5 runs, medians, same result checksums):
`sum(l_quantity) over (partition by l_suppkey order by l_orderkey)` 0.46–0.64 s → 0.14–0.15 s;
the same with `partition by l_partkey` 0.19–0.22 s → 0.13–0.15 s; a running sum without
PARTITION BY over 20K rows 0.27–0.29 s → 7–8 ms, and over all 6M rows 0.17–0.18 s (main doesn't
finish). A sliding `rows[-5,5]` frame still recomputes each frame and is unchanged (0.21–0.24 s
both when measured first; it varies with JIT warm-up order). A sliding min/max structure and suffix
frames (`CURRENT ROW AND UNBOUNDED FOLLOWING`) are possible follow-ups.
Sliding ROWS frames of counts and Int64 / Decimal sums now add the rows entering the frame and
remove the rows leaving it, after a per-partition check that no frame's fold can overflow a Long
(A/B against main, interleaved, 3 rounds of 5 runs, medians, same result checksums, partition by
l_suppkey order by l_orderkey): `sum(l_quantity)` over `rows[-5,5]` 0.24–0.36 s → 0.11–0.12 s,
over `rows[-200,200]` 0.54–0.56 s → 0.08–0.09 s; `count(l_quantity)` over `rows[-50,50]`
0.33–0.37 s → 0.09 s. Float64 sums, avg, and min / max over sliding frames still recompute each
frame.

## Milestones (one PR each)

1. uni 2026.3.0 migration
2. Engine core: vectors, expression compiler, Values / Parquet scan, Filter, Project, Limit,
   Sort, HashAggregate; `wvlet` connector wiring; spec/basic subset conformance
3. Joins, set operations, distinct, subqueries (IN / EXISTS / scalar); TPC-H 22/22 match
   DuckDB on spec/tpch/data
4. Window functions, unnest, pivot, string/date/regex function library; spec/basic coverage
5. Parquet/Iceberg datasets with partition + stats pushdown; write path (`save to`)
6. Performance: parallel scans/aggregation/join, benchmark harness vs DuckDB
7. TPC-DS coverage
