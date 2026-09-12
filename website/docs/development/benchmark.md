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
`TPCHSchemalessTest` executes all 22 queries compiled this way on DuckDB. Other keyword-named
member calls on an untyped qualifier are rejected at compile time, since `x."keyword"(...)` is
never valid SQL.
