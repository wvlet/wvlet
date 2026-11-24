# Engine: build hash join tables in parallel

## Goals

- Cut the TPC-H SF10 multi-way join gap (q21 6.5x, q4 7.5x, q7 6.8x, q18 4.6x DuckDB on main 3a2984f6), where under 4 of 18 cores stay busy.
- One focused fix: the serial part of the hash join build over `lineitem` / `orders`. q17 and q10 are left for a later PR (see Background).

## Background

- Profiled at SF10 on main 1c986842 (async-profiler, 3 timed runs plus warm-up per query, engine threads only).
- `JoinBuild.ensureBuilt` runs on the calling thread, in the `prepare` step of the probe's `MorselOp`, before any morsel starts. Every pool thread waits while it runs.
- Calling-thread time spent in the build, per run:

| query | engine ms (DuckDB) | busy cores engine vs DuckDB | build on calling thread | build key |
|---|---:|---|---:|---|
| q21 | 1,231 (198) | 5.1 vs 14.3 | ~840 ms (68%) | `l_orderkey`, 60M rows |
| q4 | 232 (70) | 6.1 vs 9.7 | ~160 ms (69%) | `l_orderkey` |
| q7 | 433 (105) | 7.9 vs 14.2 | ~155 ms (36%) | `o_orderkey`, `l_orderkey` |
| q18 | 471 (154) | 12.2 vs 13.9 | ~120 ms (25%) | `o_orderkey` |
| q17 / q10 | 829 / 890 (102 / 111) | 12.3 / 9.3 vs 13.9 / 12.6 | ~40–55 ms (5%) | (not build-bound: probe, Parquet and GC) |

- All of these builds take the direct index (#2354): a dense integer key such as orderkey. Under `buildTable` in q21:
  - direct-index insertion (`buildDirect`, random writes into a 240 MB `heads` array): 51%
  - `Batch.concat` of the build input (`Operator.materialize`): 20%
  - the per-row NULL-key scan (`GroupKey.hasNull`, a virtual call per key per row): 16%
  - `heads` / `chain` fills: 7%
  - the runtime join filter (`RuntimeFilter.set`) scans min/max over the same rows again, serially.
- The build's input scan already runs on the pool; only the work after it is serial. Hashed and flat builds (`buildHashed`, `LongJoinTable`) already insert partitions in parallel; the direct index does not.

## Design

Lane: Standard — an executor change on the hot path of every large join, measured at SF10.

- New toggle `parallel_join_build` (on by default), set on each `JoinBuild` the way `join_direct_index` is, and copied where `Parallelizer` rebuilds the join (both `withDirectIndex` sites). Off reproduces today's build.
- Run the steps after the input scan in parallel on the query's `TaskRunner`, for builds over a row threshold (`JoinBuild.MinParallelRows`); small builds stay serial.
  - **Concat:** a `JoinBuildSteps.concat` that takes a runner and copies each input batch into the concatenated columns as separate tasks (typed arrays only; boxed columns keep today's copy). Other callers (Sort, nested loop) keep the sequential default.
  - **NULL-key scan:** when no key column has NULLs, `valid` is the identity (a parallel fill), so `RuntimeFilter.set`, `KeyPacker.of`, and the build paths keep one input shape. Otherwise row chunks are scanned in parallel and joined in order.
  - **Min/max:** computed over row chunks in parallel for the runtime filter (raw keys) and the direct index (normalized keys).
  - **Direct index:** split the key range into slices, one per task. Each row chunk counts its rows per slice; a prefix sum gives each (chunk, slice) an offset; chunks scatter their rows into one index array (nv ints, about 240 MB extra peak in q21, next to the 240 MB `heads`), so each slice's rows stay in input order. Each task then fills its slice of `heads` and the `chain` of its rows, so no two tasks write the same slot and chains keep input order. This does not reuse `GroupTable.partitionRows`, which is serial and allocates an array per partition.
- Results stay identical: same `heads`, `chain`, and `uniqueKeys` as the serial build, so the probe side does not change.
- Running builds of different joins at the same time (several `prepare` steps in q7) is out of scope.
- Tests: the parallel and serial builds give the same tables and join rows (NULL keys, duplicate keys, skewed slices, a forced small threshold), plus the toggle in `OptimizerOptionsTest` and `EngineOptimizerToggleTest` (which checks that the off setting reaches the shared build after `Parallelizer`).
- Scala.js runs `TaskRunner` sequentially, so no platform-specific code is needed; no Lean-modeled area is touched. No plan change, so golden plans stay as they are.
- Measure with `WVLET_ENGINE_BENCH_AB=parallel_join_build`: SF10 TPC-H and TPC-DS (RUNS=3), SF1 both suites (RUNS=5), and changed queries again with RUNS=10.

## Alternatives and Why Not?

- **Build partitions of the direct index with CAS on `heads`.** No partitioning pass, but chains lose input order and CAS on a 240 MB array is slower than owning a slice.
- **Skip `Batch.concat` and index into the input batches.** Saves the copy, but every probe gather would need a (batch, row) lookup; larger change for 20% of the build.
- **Start probing while the build runs (pipelined builds).** Larger change to the morsel scheduler; parallelizing the build gets most of the gain first.
