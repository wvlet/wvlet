# Engine: run the hash join builds of one pipeline at the same time

## Goals

- Follow-up to #2360 (`parallel_join_build`) for the TPC-H SF10 multi-way joins: q21 (2.8x DuckDB at SF10 on main 55c1bc12) and q7 (7.4x) keep 8 of 18 cores busy.
- One focused fix: the builds of different joins in one pipeline run one after another in `MorselOp.prepare`, so the serial parts of each build add up.

## Background

- Profiled at SF10 on main 55c1bc12 (async-profiler, `cpu` with `threads`, one JVM per query, warm-up plus 3 runs).
- q21 at SF10 has one probe pipeline (over `orders`) with three builds: the inner join over `lineitem ⋈ supplier`, a semi join over all of `lineitem` (60M rows) and an anti join over late `lineitem` rows. `Parallelizer` chains them as `prepare :+ ensureBuilt`, and `MorselOp.next` runs the list in order on the calling thread.
- The calling thread's CPU per run (profiled, under load): q21 ~450 ms in `JoinBuild.buildTable`, of which ~270 ms is `__bzero` zeroing the new build arrays (concat columns, `chain`, `heads`, valid rows) and ~60 ms Parquet footer parsing when the build input opens. The pool idles meanwhile. q7: ~190 ms in builds (~105 ms zeroing).
- Each build's input scan is parallel, but the steps between scans (open, zeroing, the tail of one scan before the next build starts) are serial, three times over in q21.
- q17 and q10 are not build-bound (build ≤5% on the calling thread; probe, Parquet decoding and GC), so they stay for a later PR.
- Parquet footer parsing on every open (~50–90 ms per query across planning and scans) is another serial cost, but caching parsed footers needs a uni API to open a file from a parsed footer (the `ParquetFile` constructor is package-private).

## Design

Lane: Standard — an executor change to how every multi-join pipeline starts, measured at SF10.

- New toggle `concurrent_join_builds` (on by default), passed to `Parallelizer` like `parallel_agg_partitioning`. Off keeps the sequential `prepare`.
- `Parallelizer.prepareSteps`: when a pipeline has more than one prepare step, its `MorselOp` (and the two-phase aggregation over it) gets a single step that calls `TaskRunner.runTogether(runner, steps)`.
- `TaskRunner.runTogether`: runs the first step on the calling thread and the others on coordinator threads (`TaskRunner.coordinate`, outside the task pool, as for shared CTEs), each wrapped in a `ComputeOnce`. The caller then takes each step in order: it waits for the ones a coordinator has started and runs the ones not yet started itself. Pool threads never wait for coordinator work, and the caller never waits for queued work, so the bounded coordinator pool cannot deadlock nested pipelines.
- Failures: the first failure is raised after every started step finishes; steps not started by then never start.
- Runtime join filters: a build sets its filter on a probe-side scan, read through a `@volatile`; a scan that starts before it reads without it (as today). Builds of one pipeline do not feed each other's inputs, since later (upper) builds already ran after the lower ones' input scans finished.
- Memory: the builds' inputs and tables are alive at the same time during the build phase instead of one after another; all are kept until the probe ends anyway.
- Scala.js: `coordinate` runs the task when submitted, so steps keep their order. No Lean-modeled area is touched, and plans don't change, so golden plans stay as they are.
- Tests: `runTogether` order, failures and nested use with a small coordinator pool; the toggle in `OptimizerOptionsTest` and `EngineOptimizerToggleTest`.
- Measure with `WVLET_ENGINE_BENCH_AB=concurrent_join_builds`: SF10 TPC-H and TPC-DS (RUNS=3), SF1 both suites (RUNS=5), and changed queries again with RUNS=10.

## Results

`WVLET_ENGINE_BENCH_AB=concurrent_join_builds` (on / off, one JVM, load average 12–19):

| suite | runs | wall | CPU | geomean |
|---|---:|---:|---:|---:|
| TPC-H SF10 | 3 | 0.951 | 1.006 | 0.961 |
| TPC-DS SF10 | 3 | 0.976 | 0.987 | 0.965 |
| TPC-H SF1 | 5 | 0.947 | 1.005 | 0.964 |
| TPC-DS SF1 | 5 | 0.935 | 0.995 | 0.919 |

- Rechecked with RUNS=10 at SF10: TPC-H q21 0.86 (437 vs 510 ms), q18 0.91, q2 0.72; q7 1.03 and q10 0.99 (no gain); controls q1 / q3 / q4 / q6 within 0.99–1.02. TPC-DS q83 0.65, q33 0.87; the RUNS=3 regressions (q34 1.54, q25 1.40, q95 1.30) were noise (1.00, 1.02, 0.96).
- q7 keeps its gap: its builds are small next to the probe, so overlapping them saves little.

## Alternatives and Why Not?

- **Allocate the large build arrays on pool threads, concurrently.** Cuts the zeroing per build only (at best to the largest array), while overlapping whole builds hides zeroing, opens and scan tails together.
- **Cache parsed Parquet footers.** Helps every query, but needs a uni release with an API to open a file from a parsed footer; a later PR.
- **Start probing while builds run (pipelined builds).** Larger change to the morsel scheduler.
