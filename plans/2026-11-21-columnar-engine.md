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
| Parallel right / full joins, outer joins build on the smaller side (all 22 queries) | 2.0 s | 5.2x |

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
