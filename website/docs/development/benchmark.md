---
sidebar_position: 6
---

# Benchmarking TPC-H on DuckDB

The [TPC-H](https://www.tpc.org/tpch/) queries under `spec/tpch` double as a performance workload.
CI runs them only at scale factor 0.01 as correctness specs
(`./sbt "runnerJVM/testOnly *RunnerSpecTPCH"`). The tools below time them at larger scale
factors; they are opt-in and never gate CI, because the numbers are machine-bound and only
meaningful as before/after comparisons on one machine.

## In-process timing probe

`TPCHBench` compiles every `spec/tpch/q*.wv` with one compiler and executes it through the
regular runner on an in-memory DuckDB. It runs only when the scale factor is given in the
environment (sbt forks the test JVM, so a `-D` property would not reach it):

```bash
# dbgen(sf=1), one warm-up plus 3 timed executions per query (WVLET_TPCH_BENCH_RUNS to change)
WVLET_TPCH_BENCH_SF=1 ./sbt "runnerJVM/testOnly *TPCHBench"
```

The log shows the DuckDB set-up time (native library load plus `dbgen`), then one line per query
with the compile time and the min/median/max execution time, and finally the sum of the medians:

```
DuckDB setup with dbgen(sf=1.0): 1842.3 ms
q1.wv    compile    41.2 ms  exec min    88.7 ms  median    90.1 ms  max    93.4 ms
...
sum of per-query median exec time over 22 queries: 1213.5 ms
```

Execution time here includes the JDBC round trip and building the `QueryResult`, so it is what a
`wv` user experiences rather than the pure engine time.

## Columnar engine vs DuckDB

`EngineTPCHBench` runs the same `spec/tpch` queries on the in-process columnar engine
(`wvlet-engine`) and on DuckDB, both reading the same Parquet files, and reports the median time
of each and their ratio. The data is generated once per scale factor with DuckDB's `tpch`
extension into `target/tpch-sf<sf>/<table>.parquet` (never committed) and reused afterwards:

```bash
# one warm-up plus 3 timed executions per query on each side
WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"

# a subset of the queries, with a single-threaded DuckDB for a like-for-like comparison
WVLET_ENGINE_BENCH_SF=1 WVLET_ENGINE_BENCH_QUERIES=q1,q6 WVLET_ENGINE_BENCH_DUCKDB_THREADS=1 \
  ./sbt "engineJVM/testOnly *EngineTPCHBench"
```

`WVLET_ENGINE_BENCH_RUNS` sets the number of timed runs. The log ends with a Markdown table and
the same numbers are written to `target/engine-bench/tpch-sf<sf>.tsv`. Engine time covers
planning and execution of an already compiled query; DuckDB time covers running the generated
SQL over JDBC and reading every result row. Row counts are compared too, and a mismatch is
flagged in the table. The `engine plan (ms)` column is the median time before the first operator
runs: model expansion, planning, and the subqueries and statistics reads done while planning
(scalar and IN subqueries, materialized CTE estimates), so a small query's fixed cost shows apart
from its execution.

## Columnar engine on Scala Native

`spec/tpch/bench/run_native_bench.sh` runs the `spec/tpch` queries on the engine inside a Scala Native `wvc` binary, one process per query, and prints each run's wall time, peak RSS, and exit status. A crash or an OutOfMemoryError shows up as a non-zero status followed by the process's error output. The wall time covers the whole process: start-up, compilation, planning, execution, and printing the result.

```bash
# release-fast build; SCALANATIVE_GC=boehm builds the Boehm variant for comparison
SCALANATIVE_MODE=release-fast SCALANATIVE_GC=immix ./sbt wvc/nativeLink
cp target/out/native0.5/scala-*/wvc/wvc target/wvc-immix
# 5 rounds of q1-q22 on the SF1 data that EngineTPCHBench generates (spec/tpch/data for tiny data)
spec/tpch/bench/run_native_bench.sh target/wvc-immix target/tpch-sf1 5
```

On Scala Native 0.5.13 (Apple M5 Pro, 18 cores, 48 GB, 5 alternating rounds per GC), all 22 queries completed in all 220 runs. The sum of the per-query median wall times was 6.0 s with Immix and 9.9 s with Boehm. Peak RSS was 981 MB with Immix (median over queries 421 MB) and 583 MB with Boehm (196 MB). Immix's heap grows past the live data, and `GC_MAXIMUM_HEAP_SIZE` bounds it: 768M ran every query at 806 MB peak and about 10% slower, while 512M and 256M ran out of heap on 2 and 15 of 44 runs. The engine's live data grows with the input, so `wvc` ships with no cap. Set `GC_MAXIMUM_HEAP_SIZE` to bound its memory.

## Manual recipe: `EXPLAIN ANALYZE` on the duckdb CLI

For engine-level numbers, or to compare against hand-written SQL, run the generated SQL directly
on the [duckdb CLI](https://duckdb.org/docs/installation/) against a persisted database.

1. Build the database once (sf=10 takes a few minutes and about 3 GB):

   ```bash
   duckdb target/tpch_sf10.duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf = 10);"
   ```

2. Dump the SQL that Wvlet generates for each query. Run from `spec/tpch` so that `schema.wv`
   is picked up and every column is typed:

   ```bash
   mkdir -p target/tpch-sql
   (cd spec/tpch && for q in q*.wv; do
      wvlet compile -f "$q" | grep -v '^--' > "../../target/tpch-sql/${q%.wv}.sql"
   done)
   ```

3. Time each query with `spec/tpch/bench/run_bench.sh <db> <sql_dir> [runs] [threads]`. It runs
   `EXPLAIN ANALYZE` per query and prints a TSV with the min and median engine time:

   ```bash
   spec/tpch/bench/run_bench.sh target/tpch_sf10.duckdb target/tpch-sql 3 8
   ```

Point step 3 at a directory of canonical TPC-H SQL to get the baseline for the same database.

## Compiling without a schema

`wvlet compile` from a folder without table declarations cannot type the columns, so member
calls such as `o_comment.like('%x%')` are not inlined by the analyzer. The SQL generator lowers
the keyword-named ones (`like`, `in`, `not_in`, `between`, `extract`) to their operator form, and
`TPCHSchemalessTest` executes all 22 queries compiled this way on DuckDB. Other member calls keep
the plain function-call form, because a schema-qualified call such as `main.left(...)` has the
same shape and is valid SQL.
