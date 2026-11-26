# Columnar engine

This page owns how `wvlet-engine` plans and executes Wvlet relations, and the gates an engine change must pass. Why the engine exists and why it executes typed plans instead of SQL is in [the columnar engine ADR](../adr/2026-11-21-columnar-engine.md); its optimizer decisions are the `adr/2026-11-2*-engine-*.md` records.

## Planning path

- Every relation is planned through the runtime IR in `wvlet.lang.engine.runtime`: `RuntimeBinder` → `RuntimePlan` → `Lowering` into physical operators (`wvlet.lang.engine.exec`).
- `Planner` (`wvlet.lang.engine.plan`) keeps the shared pieces the binder calls, generic over a plan builder: correlated joins, decorrelation, join ordering, and parallelization.
- Logical passes (`PushDownFilters`, `PruneColumns`, `ReorderJoins`, `GroupKeySemiJoins`, `ShareSubplans`, `Normalize`, ...) run in `Optimizer` over the runtime plan. Rules with three-valued-logic subtleties have Lean models; see [formal verification](formal-verification.md).
- `WvletEngine.runtimePlan` uses a new `Planner` per call; share one `Planner` when building plans by hand in tests.
- `Lowering` rewrites expressions over positions, after the logical passes: `fold_constants` folds row-independent subexpressions and pre-scales decimal constants, and `common_subexpressions` computes a subexpression repeated across a Project, Filter, Aggregate, or Window once in a Project below it (`CommonSubexpressions`). It shares only subexpressions evaluated for every row in at least one place, never volatile functions, and matches with `==`, not `sig`.
- `FilterOp` evaluates an AND / OR condition with `expr/Selection.scala` (`short_circuit_filters`): each conjunct runs only on the rows the earlier ones kept, each disjunct only on rows not yet accepted, so a costly conjunct (string IN, LIKE, a function) is not evaluated on rows already removed. Filters with cheap parts only keep the full-batch masks, and cheap leaves run over the whole batch, so a failing cast may still see removed rows, as before. `reorder_conjuncts` runs cheap parts first by a static cost; a part that can throw (`Selection.isSafe` is false) is a barrier no part moves across, so it never sees more rows than in the written order. With `dictionary_selection`, a part over one dictionary-coded string column (`dictionary_filters`, now also a whole AND / OR over that column) runs once per dictionary entry of the rows still selected and returns their positions (`Selection.DictLeaf`), and an AND with two such parts selects even though every part is cheap (TPC-DS q41). Elsewhere (projections, CASE, join residuals) AND / OR still evaluate both sides over the whole batch.
- Two-phase aggregation merges identical partial aggregates (`shared_agg_partials`), so `avg(x)` reuses `sum(x)` and `count(x)`.
- With `aggregate_selected_rows`, `HashAggOp` reads deferred input columns (a filter's or join probe's `Batch.take` output) at their positions instead of gathering them (`exec/SelectedInputs.scala`): sum / min / max / count over an Int64, Double, or Long-backed decimal column (`Accumulator.updateSelected`), `+` / `-` trees over such columns into one array (a decimal Long overflow evaluates the tree as before; Int64 overflow throws as before), and packed group keys (`PackedGroupTable.groupIdsAt`). Position -1 reads as NULL. With `fused_accumulators`, arguments are evaluated up front only for batches the fused loop folds. Other aggregates (FILTER, DISTINCT, several arguments) gather as before.
- The binder simplifies each expression as it binds it (`plan/BoundExprSimplifier.scala`), before the logical passes: `fold_bound_constants` turns nodes over literals into literals (so `x <= 1200 + 11` reaches scan filters) and simplifies `and`/`or` with a literal operand; `unwrap_casts` compares `x` instead of `cast(x as T)` with literals when the cast is lossless (integer widening, integer or decimal to a wide enough decimal) and every literal converts exactly. A failing evaluation (`1 / 0`) stays unfolded, so it still fails only when rows exist.
- Scan filters (`plan/ScanFilters.scala`, `push_scan_filters`) only skip Parquet row groups, so a translation must keep every row group the engine's own comparison could keep. With `decimal_float_scan_filters` they also compare decimals (checked by `catalog/DecimalStats.scala` for single-file scans, since uni treats DECIMAL statistics as always matching) and floating-point `<`, `<=`, `=`; never `>` or `>=` on floats, because the engine orders NaN above every number (`Double.compare`) and writers leave NaN out of the statistics.
- With `parallelize`, INTERSECT / EXCEPT (without ALL) become semi / anti hash joins over the left input's distinct rows on every column, with NULL keys equal (`set_op_joins`, `HashJoinOp.nullsEqual`). A build with a NULL key skips the packed, direct, and flat paths; a join with NULL-equal keys takes no runtime filters.
- With `parallelize`, a UNION ALL of scan pipelines is one pipeline (`union_all_pipelines`): one `MorselOp` runs the parts of every input, in input order and then part order, through the operators above the union (hash join probes over one shared build, partial aggregates). An input whose source has a single part runs on the calling thread in its turn; a union with an input that is no pipeline, or has right / full joins, keeps its inputs' own pipelines.
- Semi / anti joins of EXISTS / IN subqueries build on the outer input (RIGHT SEMI / RIGHT ANTI) whenever the subquery input is estimated larger (`semi_join_build_side`); inner and outer joins swap only at 4x (`JoinOrderOptimizer.SwapBuildRatio`). A subquery that aggregates or deduplicates keeps the 4x rule, as row estimates pass through filters and aggregations (`CardinalityEstimator.rows`, `Stats.rows`); lineitem vs orders thus compare at their source ratio.
- The join search estimates a range comparison of an integer or date column with constants (`<`, `>`, `between`, date ranges) from the column's [min, max] in the Parquet footer, with the bounds on one column as one interval (`range_selectivity`, `runtime/RangeSelectivity.scala`); decimals have no footer range in uni and keep the default 0.3. `Stats.rows` and `CardinalityEstimator.rows` still pass source rows through filters and joins, so build-side choices outside the join search (semi joins, and the semi joins `reduce_semi_joins` and `group_key_semi_joins` add) don't see these estimates; `group_key_semi_joins` does read them for the size of its semi-join source.
- `group_key_semi_joins` also reduces an aggregation on the left of an inner join by swapping the inputs with the same build side (`left_group_key_semi_joins`), so the copy of the semi-join source is lowered after the original.
- `eager_aggregation` (`runtime/EagerAggregation.scala`) computes an aggregation below the top inner join under it, by the aggregated side's join keys and the group-key columns it holds, when the calls read one side and can't fail on rows the join drops (sum, min, max and counts over columns, `+ - *`, division by a nonzero constant, widening casts), the partial groups are at most 1/4 of that side's `Stats.treeRows` and at most 4x the outer groups, and the other side keeps ≥ 70% of its rows. Shared CTE bodies run it again in `SharedCTE.lower` once their readers' filters are pushed in (TPC-DS q04, q11).
- `cte_filters_through_filters`: the readers' filters pushed into a shared CTE body (`Optimizer.pushFilters`) move below the body's own filters when `Selection.isSafe` says they can't throw, so they reach the joins and scans under them (TPC-DS q64: `syear in (1999, 2000)` below the `cd_marital_status <>` filter, down to `date_dim`). Conjuncts that can throw stay above.

## Generated TPC-H tables

- `TpchGen` (`catalog/`) generates the TPC-H tables as `MemTable`s on every platform, with spec row counts, keys, and value domains but not `dbgen`'s values; the web playground queries them in the browser. `TpchGenQueriesTest` runs every spec/tpch query over them, and `PlaygroundEngineTest` runs every playground example. See [the playground ADR](../adr/2026-11-25-playground-engine-tpch-gen.md).
- Compile queries over registered tables with `EngineCatalog.toCompileCatalog(tables)` as the compiler's default catalog, so column types resolve stdlib methods (`o_comment.like(...)`).

## Optimizer toggles

- `Optimization` (`plan/OptimizerOptions.scala`) names each planner optimization. Turn them off with `wvlet.engine.optimizer.disabled=<names>`, `WVLET_ENGINE_OPTIMIZER_DISABLED`, or `WvletEngine(optimizer = ...)`.
- `EngineOptimizerToggleTest` checks results with each optimization off.
- A new toggle must be listed in `OptimizerOptionsTest`, or the JS and Native CI jobs fail.
- Give every executor or planner change an `Optimization` toggle from the start, so it can be benchmarked on one build.

## Parquet footer cache

- `ParquetFooterCache` (`catalog/`) keeps parsed footers of local Parquet files, process-wide, LRU up to 256 files. Behind `parquet_footer_cache` (on by default).
- An entry is valid while the file's size and last-modified time match a stat taken before the footer read. Symbolic links, files without an mtime, and files modified within the last 10 seconds are not cached, as DuckDB's `parquet_metadata_cache` does for coarse mtimes.
- Scans use it when `ScanSpec.footerCache` is set (from the toggle at lowering); planning reads when the `ParquetSource` was built with `footerCache` (file paths in queries). Other callers parse as before.
- `ParquetSource.estimatedDistinct` caches estimates per (path, column) with the file version from the same stat helper (`ParquetFooterCache.fileVersion`), LRU up to 4096 entries. `ParquetSource.valueCounts` (string value samples from up to 4 row groups, for `string_value_selectivity`) caches the same way in its own LRU of 512 entries. A changed version recomputes; files the footer cache skips are estimated once per `ParquetSource` (one query), not shared. It validates with or without the toggle. A source reads the file version at most once per second (`ParquetSource.VersionRecheckNanos`): join ordering asks for estimates thousands of times per query, and a stat per call took most of TPC-DS q64's planning time.
- Only local paths are cached. A remote source would need a key that changes on overwrite (e.g. an ETag), since uni's `FileInfo` has none. See [the footer cache ADR](../adr/2026-11-24-engine-parquet-footer-cache.md).

## Accelerators (GPU offload)

- A `PlanAccelerator` (`runtime/PlanAccelerator.scala`) is offered each runtime plan subtree top-down in `Lowering`, before CPU lowering; the first subtree it accepts runs as one operator. `WvletEngine(accelerator = ...)` sets one; the `gpu_offload` toggle turns the hook off. See [the GPU executor ADR](../adr/2026-11-24-engine-gpu-executor.md).
- `wvlet-engine-gpu` is the example accelerator (cudf-java, JVM only, not in the CLI pack or the CI aggregates). It runs subtrees that join or aggregate, keeps scanned columns on the device across queries, and runs a subtree on the CPU if the device fails at run time.
- `EnginePlanAcceleratorTest` checks the hook with a stub accelerator. On a machine with a CUDA device, `./sbt "engineGpu/testOnly *GpuTPCHTest"` checks spec/tpch against DuckDB with GPU offload (`WVLET_GPU_TEST_DATA=target/tpch-sf10` checks SF10), and `WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_BENCH_AB=gpu_offload ./sbt "engineGpu/testOnly *GpuTPCHBench"` compares GPU on and off.
- Optimizations inside the GPU module are `GpuFeature`s rather than `Optimization` toggles, because they don't change engine plans: A/B one with `WVLET_GPU_BENCH_AB=<feature>` in the same bench. The module rewrites only its own subtrees (implied filters, join key filters, column pruning) and picks decimal widths from cached value bounds; see [the follow-up ADR](../adr/2026-11-24-gpu-executor-runtime-choices.md). The column cache also records per cached column whether it is sorted and a dictionary of low-cardinality strings, for sorted and coded group-bys; see [the column properties ADR](../adr/2026-11-24-gpu-column-properties.md).
- Tables over `WVLET_GPU_STREAM_ROWS` rows (default 100M) whose whole scan wouldn't fit stream through the GPU subtree in row ranges: partial aggregates merge on the device, joins probe a hash table built once, and string columns over cudf's 2 GB limit are cached in pieces. A subtree that would concatenate such a string column stays on the CPU at plan time. `GpuStreamTest` streams spec/tpch in small ranges against DuckDB; see [the chunked scans ADR](../adr/2026-11-24-gpu-chunked-scans.md).

## Correctness gates

- `EngineBasicSpecTest` runs spec/basic with `PlanValidator` on.
- Engine tests check rows against DuckDB through generated SQL (`EngineConformance.compare`), with fixed rows where DuckDB evaluates a form differently (for example `sample`, `describe`).
- Shared engine tests also run on `engineJS`; Scala.js prints `-0.0` as `0`.

## Golden plans

- Physical plans of spec/tpch and spec/tpcds are golden files under `spec/engine-plans/` (TPC-DS at SF0.1, TPC-H on the tiny `spec/tpch/data`). `EnginePlanGoldenTest` fails on drift.
- After an intended planner change, regenerate them: `./sbt "engineJVM/Test/runMain wvlet.lang.engine.EnginePlanGolden"`.
- SF1 plans can differ from the golden ones for estimate-based changes; check them with `WVLET_ENGINE_BENCH_EXPLAIN=1`.

## Function coverage ratchets

Raise `minCovered` when adding support; never lower it.

- `./sbt "engineJVM/testOnly *EngineStdlibCoverageCheck"` runs every stdlib function visible to the DuckDB dialect on the engine and on DuckDB and counts matches. This is the primary measure, because the stdlib is Wvlet's engine-neutral function contract. Report: `target/engine-stdlib-coverage.txt`.
- `./sbt "engineJVM/testOnly *EngineFunctionCoverageCheck"` counts DuckDB catalog names the engine binds (`EngineFunctions`), a compatibility measure. Report: `target/engine-function-coverage.txt`.
- Add new functions under stdlib names, not by porting DuckDB names as public API; see [stdlib](stdlib.md).
- After adding or removing an engine function name, regenerate the compiler's copy of the list (`EngineFunctionNamesTest` prints the command). The compiler rejects calls outside it on the `wvlet` target ([dialect contexts](stdlib.md#dialect-contexts)). `EngineConformance` compiles for `DBType.Wvlet`, so a wrongly rejected call fails the conformance suites.

## Benchmarks

- `EngineTPCHBench` and `EngineTPCDSBench` run the spec queries on the engine and on DuckDB over the same Parquet files. They are ignored unless `WVLET_ENGINE_BENCH_SF` is set. The other `WVLET_ENGINE_BENCH_*` variables are listed in the bench classes' Scaladoc and in [the benchmark guide](../website/docs/development/benchmark.md).
- A/B an engine change behind its `Optimization` toggle in one JVM: `WVLET_ENGINE_BENCH_SF=1 WVLET_ENGINE_BENCH_RUNS=5 WVLET_ENGINE_BENCH_AB=<toggle> ./sbt "engineJVM/testOnly *EngineTPCHBench *EngineTPCDSBench"`. Read the suite on/off totals first; single queries are noisy.
- A change that cannot be toggled is benchmarked against an origin/main worktree, alternating rounds between the two trees.
