# Engine query optimizer framework

Status: accepted 2026-11-22 (all decisions below as proposed). Implementation follows the milestones in section 7.

## Goals

- Give `wvlet-engine` one place where logical rewrites happen, so that new rules don't depend on where they sit inside the recursive `Planner`. The planner is 2.8k lines today.
- Make join order the one cost-based search, with an exact enumerator for small join graphs and today's greedy as the fallback.
- Keep the framework small enough for Scala.js and Scala Native (code size, single thread), and testable one rule at a time.
- Migrate in steps. Each step keeps TPC-H 22/22 and TPC-DS 99/99 conformance and does not regress `EngineTPCHBench` / `EngineTPCDSBench` (sf=1: about 4.4x and 3.5x DuckDB today).

Non-goals: a general Cascades/memo search, a rule DSL with code generation, and changing the SQL that `GenSQL` emits for DuckDB/Trino/Snowflake.

## Background: what the engine does today

`WvletEngine.execute` runs `GenSQL.expand` (model references inlined), then `Planner.plan`:

```
Relation (wvlet-lang model.plan, typed, names not ids)
  └─ prefetchScalarSubqueries            plan-time execution of merged scalar subqueries
  └─ planRelation                        binds names → positions AND optimizes AND lowers, in one recursion
       └─ Binder / BindScope / WindowCollector   expressions → VExpr (ColRef by position)
  └─ ColumnPruner                        physical pass over Operators (also attaches Parquet row-group filters)
  └─ Parallelizer                        physical pass: MorselOp pipelines, two-phase / partitioned aggregation
```

There is no logical IR between the compiler's `Relation` and the physical `Operator`s. Every optimization is made while lowering, so it can only see the local subtree and the mutable planner state (`cteDefs`, `cteRefCounts`, `scalarResults`, `prefetchTried`, `hiddenSeq`).

### Catalog of current optimizations

| Optimization | Where | Rewrites | Depends on / ordering |
|---|---|---|---|
| Conjunct splitting + template expansion | `Planner.conjuncts` | `Relation` expressions | Must run before every filter/join decision; `Binder.expandTemplates` first so that `sql"..."` stdlib templates become real expressions |
| OR factoring `(a∧b)∨(a∧c) → a∧(b∨c)` | `Planner.factorOr` | `Relation` expressions, matched by `exprName` text | Before join-key extraction, so keys inside OR branches become equi-keys (TPC-H q19, TPC-DS q13/q48) |
| Join-graph flattening | `flattenJoins` | `Relation` inner joins → leaves + conjuncts | Stops at outer joins, ASOF, USING/NATURAL, lateral unnest |
| Filter pushdown to join inputs | `planJoinTree` | Single-input conjuncts are applied to the already-*lowered* leaf `Operator` | Needs leaves lowered first, to know their schemas |
| Pushdown into the preserved side of LEFT JOIN | `planFilter` | `Relation` (re-plans `j.left` just to learn its schema) | Special case so the inner-join tree under a left join sees the filters |
| Equi-key extraction | `planJoinTree` (`CrossCond`, `sidesOf`, `joinKeys`) | `Relation` conds → bound `VExpr` keys with casts to a common type | Input refs computed by *trial binding* (`bindsIn`, which catches `ColumnNotFound`) |
| Greedy join order (multi-start, C_out + build rows) | `planJoinTree.greedy` (#2233, #2235, #2239) | Builds `HashJoinOp` / `NestedLoopJoinOp` directly | Estimates read from physical ops (`estimateRows`, `columnDistinct`); build/probe swap (`SwapBuildRatio`) decided inline |
| Selectivity / ndv | `selectivity`, `keyDistinct`, `columnDistinct` | Reads `ScanOp.source.estimatedDistinct` through Filter/Project/HashJoin only | Other operators give no distinct counts; `estimateRows` passes through the first child's rows (an aggregate counts as its input), 1e6 for a scan without row stats |
| Uncorrelated scalar subquery as constant | `bindScalarSubquery`, `evalScalarSubquery` | Executes the subquery *during planning* and caches the result | `withoutScalarSubqueries` replaces them with NULL to decide pushdown (#2229) |
| Shared scalar-subquery inputs | `prefetchScalarSubqueries` (#2243) | Groups subqueries by the *generated SQL text* of their input, merges, executes | Must run before `planRelation`; skips anything that may read a CTE because CTE scope is only known while planning |
| EXISTS / IN / NOT IN decorrelation | `rewriteSubqueryConjunct`, `existsJoin`, `semiJoin`, `inJoin` | Conjunct → semi/anti hash join on lowered operators | Correlation detected by planning the subquery and catching `ColumnNotFound` (`isCorrelated`) |
| Correlated scalar / EXISTS under OR, NOT, CASE | `bindWithSubqueries`, `decorrelateScalar`, `decorrelateExists` | Left join + flag/value column, then projected away | Runs after semi-join rewrites, re-binds earlier conditions over the widened input |
| Shared CTEs | `countCTERefs`, `planTable` (#2229, #2240) | CTE referenced >1 time → `SharedResult` (lazy materialization, readers by position) | Each CTE body is pruned and parallelized on its own (`Parallelizer(ColumnPruner(...))`) so pruning never crosses a CTE boundary |
| Grouping sets | `expandGroupingSets`, `groupByVaryingKeys` (#2229, #2238) | One `HashAggOp` per set over a shared input; constant keys re-added by projection | Shared input is a `SharedResult`, pruned to the keys/aggregates used |
| Column pruning | `ColumnPruner` (#2204, #2222, #2234) | Physical `Operator`s, positions remapped | Runs after lowering; stops below operators it doesn't know; any `IllegalStateException` → *whole plan unpruned* |
| Row-group pruning | `ColumnPruner` + `ScanFilters.toParquet` (#2204) | `FilterOp` *directly* over a `ScanOp` → `ParquetFilter` | Only fires when nothing sits between the filter and the scan |
| ORDER BY matching to select items | `resolveSortItems` (#2223) | Matched by SQL text (`exprText`) | — |
| Parallel pipelines, two-phase agg, UNION distinct | `Parallelizer` (#2210, #2241, #2242, #2249) | Physical pattern rewrites | Case order matters (see below) |

### Pain points from the history (`git log -- wvlet-engine`, 45 commits)

1. **Pattern-order bugs.** In #2249, the `LimitOp`-over-`DistinctOp` case in `Parallelizer` "never matched" because the generic `LimitOp` case came first. Nothing warned about the dead case. A rule list with explicit priorities and a "rule fired" counter in tests catches this class of bug.
2. **Passes that silently stop.** `ColumnPruner` had no `WindowOp` case, so a window over lineitem read all 16 columns (#2222). Set operations had the same gap (#2234: q38 386→75 ms). It also drops all pruning for the whole plan on an expression it can't remap. Each new operator needs a pruner case, and missing one costs speed without any failure.
3. **Trial binding as analysis.** Input references, correlation, and "binds on the left side" are all answered by binding and catching `ColumnNotFound`. With ~18 inputs this made q64 planning go from 16 to 293 ms until #2239 precomputed `CrossCond`. It is still used in `isCorrelated`, `bindsIn`, and `referencesInput`.
4. **Text as identity.** OR factoring, ORDER BY matching, and scalar-subquery merging compare expressions or subplans by printed SQL. Equal text can mean different columns under different scopes. #2243 had to exclude every input that might read a CTE for this reason.
5. **No boundary between logical and physical decisions.** Join order, build-side swap, and pushdown are decided while operators are being built, and estimates walk physical operators. A rule that needs a decision from later (e.g. pushing a filter into a CTE body, or transitive equalities `a=b ∧ b=5 ⇒ a=5`) has no place to go.
6. **Plan-time execution.** Scalar subqueries and merged subqueries execute inside `Planner`. This is a form of adaptivity, but it makes `explain` run queries and makes plan tests depend on data.
7. **Phase coupling across CTEs.** Each shared CTE is pruned and parallelized alone, so its readers' column needs and filters never reach its body.

None of these are wrong results today: the conformance suites catch those. They are costs to speed and to the time it takes to change the planner. They will grow with each rule, and three sessions are editing `Planner` in parallel right now.

## Reference designs

| System | Mechanism | Strengths | Costs | Fit for us |
|---|---|---|---|---|
| **DuckDB** ([optimizer.cpp](https://github.com/duckdb/duckdb/blob/main/src/optimizer/optimizer.cpp)) | Fixed, ordered list of ~30 passes over `LogicalOperator` (expression rewriter, filter pull-up/pushdown, IN clause, deliminator, unnest rewriter, join order, unused columns, statistics propagation, build/probe side, TopN, late materialization, join filter pushdown …). Each pass can be turned off (`disabled_optimizers`). Subqueries are flattened as dependent joins (Neumann & Kemper, "Unnesting Arbitrary Queries", BTW 2015). Join order: DPhyp ([Moerkotte & Neumann, SIGMOD 2008](https://dl.acm.org/doi/10.1145/1376616.1376672)) below `approximate_join_order_threshold` (12 relations), with a 10,000-pair budget and a greedy fallback ([join operations guide](https://duckdb.org/docs/current/guides/performance/join_operations)); cardinalities from HyperLogLog distinct counts with few other statistics (Ebergen, "Join Order Optimization with (Almost) No Statistics", MSc thesis, CWI 2022). [Late materialization](https://github.com/duckdb/duckdb/blob/main/src/optimizer/late_materialization.cpp) re-joins by row id under small LIMIT/TopN | Simple to read and debug; pass order is explicit; cheap at plan time; each pass is a plain tree walk. Same embedded, in-process shape as us | Pass order is hand-maintained; some passes run twice (filter pushdown) to reach a fixpoint; no global search | **Closest fit.** Our engine already copies its join-order cost (C_out) and estimators |
| **Trino** ([IterativeOptimizer](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/iterative/IterativeOptimizer.java), [PlanOptimizers](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java)) | A long list of optimizer stages. Many are `IterativeOptimizer`s: sets of `Rule<T>` with a `Pattern`, run to fixpoint over a `Memo` that holds one plan per group (`GroupReference`), with a time budget. `ReorderJoins` enumerates partitions of up to `optimizer.max-reordered-joins` (default 8) sources with `StatsCalculator` / `CostCalculator`, plus `DetermineJoinDistributionType` | Rules are small, local, and unit-testable (`RuleTester`); the memo makes stats lookups per group cheap; golden cost-based plan tests for TPC-H/TPC-DS | Large framework (pattern library, memo, lookup, stats rules per node). Rules interact through the fixpoint in ways that are hard to predict. Hundreds of rules | The rule shape and the per-rule tester are worth copying. The memo and pattern library are not worth the code size |
| **Spark Catalyst** ([Armbrust et al., SIGMOD 2015](https://dl.acm.org/doi/10.1145/2723372.2742797), `RuleExecutor`) | Batches of rules; each batch runs `Once` or `FixedPoint(100)` (`spark.sql.optimizer.maxIterations`). Rules are Scala partial functions on an immutable tree. CBO join reorder by DP up to `spark.sql.cbo.joinReorder.dp.threshold` (12). [AQE](https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html) re-plans at shuffle stage boundaries using runtime sizes (switch to broadcast join, coalesce partitions, skew splits). `spark.sql.optimizer.excludedRules` and plan-change validation help debugging | Partial-function rules in Scala, so it is idiomatic for us. Batches give structure without a memo. AQE gives real cardinalities | Fixpoint batches hide ordering bugs until a rule oscillates; the idempotence checks exist because this happened. Many rules re-traverse the whole tree on every iteration | The batch idea, `excludedRules`, and plan validation fit. AQE fits in a simpler form: our materialization points (shared CTEs, hash builds, scalar subqueries) already give exact counts |
| **Calcite** ([Begoli et al., SIGMOD 2018](https://dl.acm.org/doi/10.1145/3183713.3190662)) | `HepPlanner`: rules applied in a programmed order (`HepProgram`), no cost. `VolcanoPlanner`: Volcano/Cascades memo with traits and cost-based search (Graefe, "The Cascades Framework for Query Optimization", 1995) | Hep is a clean form of "fixed passes of rules". Volcano is fully general | Volcano is known to be hard to tune (search-space blowup, rule bugs that show up as missing plans); most users run Hep for normalization and Volcano only for physical choice | Hep's model is the right one for us; Volcano is not |
| **CockroachDB Optgen** ([blog](https://www.cockroachlabs.com/blog/building-cost-based-sql-optimizer/), `pkg/sql/opt/optgen`) | A DSL (`.opt` files) generates Go code: normalization rules run while the memo is built, exploration rules run as a Cascades search | Rules are concise and checkable; normalization during construction never yields a non-normalized plan | A code generator and memo; generated code is large | Too heavy for Scala.js/Native code size; normalization while building is a good idea we can use in the binder |
| **egg / e-graphs** ([Willsey et al., POPL 2021](https://dl.acm.org/doi/10.1145/3434304)) | Equality saturation: apply all rewrites into an e-graph, extract the cheapest term | No phase-ordering problem by construction | Blows up on relational plans with joins, needs a cost-guided extractor, and is research-grade for SQL | Not for plans. Possibly later for scalar expressions only |
| **DataFusion** (embedded Rust engine of similar size) | A list of logical `OptimizerRule`s applied in order, repeated up to `datafusion.optimizer.max_passes` (3) until nothing changes, then a list of `PhysicalOptimizerRule`s. Join reordering is limited | Small and understandable; the logical/physical split is clean | Weak join ordering hurts multi-join benchmarks | Very close to what we need, plus a real join enumerator |

What the reference systems agree on:

- Every one of them has a separate, bound logical IR. In that IR, columns have stable identities: DuckDB's `ColumnBinding`, Trino's `Symbol`, Spark's `ExprId`, Calcite's field index plus row type. Rules never re-resolve names.
- Cost-based search is worth it for join order and the physical join strategy. Everything else is a deterministic rewrite.
- Being able to disable a rule (DuckDB `disabled_optimizers`, Spark `excludedRules`) is the cheapest debugging and testing tool any of them has.

## Wvlet-specific constraints

- **Cross-platform.** The engine builds for JVM, Scala.js (single-threaded, bundle size), and Native, where a code-size OOM at link time is being fixed now. This rules out code generation of rules, a generic pattern library, and memo structures. A closed `enum` IR with hand-written `match` keeps code small, and Scala 3 compiles a `match` on an enum into compact code.
- **Input is the compiler plan.** The engine consumes `wvlet-lang` `Relation` after `GenSQL.expand`, about 50 relation node types, many of them surface syntax (`AddColumnsToRelation`, `ShiftColumns`, `Dedup`, `Pivot`, …). The rules should not have to handle the surface forms.
- **Compiler vs engine.** The compiler has normalization phases (`RewriteExpr`, `TrinoRewritePivot`, Hive rewrites) whose job is meaning, not speed. `RewriteExpr` is proven in Lean, with conformance traces. The SQL targets (DuckDB, Trino, Snowflake) run their own optimizers, and `wvlet compile` output is read by users. Optimization rules that change generated SQL would make that SQL harder to read for little gain.
- **Statistics.** Parquet/Iceberg provide row counts, min/max, null counts, and (computed and cached) distinct counts. Because the engine runs in process, it can also see exact cardinalities at materialization points: shared CTEs, hash-join builds, and scalar subqueries.
- **Correctness oracle.** DuckDB conformance over spec/basic, TPC-H, and TPC-DS. **Speed check:** `EngineTPCHBench` / `EngineTPCDSBench`.

## Design

### 1. Representations: three levels, binding once

```
Relation  ──Binder──▶  LogicalPlan (engine IR)  ──rewrite passes──▶  LogicalPlan  ──Lowering──▶  Operator  ──Parallelizer──▶ Operator
(compiler)   names→ids          small closed enum, ColumnId           cost-based join order      positions       (unchanged)
```

- **`LogicalPlan`**: a new closed `enum` in `wvlet.lang.engine.logical` with about 16 nodes: `Scan(source, cols, pushed: Option[BExpr])`, `Values`, `Filter`, `Project`, `Aggregate(keys, aggs, groupingSets)`, `Join(kind, cond)` with kinds inner/left/right/full/semi/anti/mark/asof, `Window`, `Sort`, `Limit`, `SetOp(kind, all)`, `Distinct`, `Unnest`, `CTE(id, body)` / `CTERef(id)`, `Apply(outer, sub, kind)` (dependent join, removed by decorrelation), and `Constant(batch)` for results computed at plan time.
- **Columns are `ColumnId(Int)`**, unique per query, with type and display name. Expressions are a bound form `BExpr`: `ColumnRef(id)`, calls, and literals. `VExpr` stays as the executable form with positions. Rules compare expressions structurally, by ids, not by printed SQL (pain point 4).
- **Binding happens once**, in a Relation→LogicalPlan binder that takes over name resolution from the current `Binder`/`BindScope`. The surface forms (`AddColumns`, `Exclude`, `Rename`, `Shift`, `Dedup`, `Count`, `Pivot`, …) normalize to the core nodes here, in the style of Optgen: build-time normalization, so no rule sees a surface form. Correlation is a fact the binder records ("this subquery references outer ids {…}") instead of a probe that catches exceptions (pain point 3).
- **Lowering** maps ids to positions and picks operators (`HashJoinOp` and build side, `NestedLoopJoinOp`, `HashAggOp`, `SharedResult` readers). It is mechanical apart from the physical choices listed in section 2.
- **`Parallelizer`** stays a physical pass over `Operator`s, as it is today. It is the most physical part of the code and the part most actively changing.

### 2. Rule mechanism: a fixed pipeline of passes, each a plain tree walk or a small local fixpoint batch

A `Pass` is `LogicalPlan => LogicalPlan` with a name. The optimizer is an explicit `List[Pass]`. Each pass is one of:

- a **hand-written traversal** for global rewrites (filter pushdown, column pruning, decorrelation, CTE inlining or sharing), as in DuckDB; or
- a **`RuleBatch(rules, maxIterations = 10)`** of local partial-function rules, applied bottom-up until nothing changes, as Spark and Calcite Hep do. Hitting the limit fails in tests and logs a warning in production, which catches oscillating rules.

Proposed order. Each pass is listed with why it sits there:

1. `Normalize` batch: constant folding, conjunct splitting, OR factoring, `NOT` push-in, trivial project/filter merge, `x = null` → `is null`. Must run first, because the later passes key on conjunct shape.
2. `Decorrelate`: `Apply` → semi/anti/mark/left joins (today's `existsJoin` / `inJoin` / `decorrelateScalar` logic, with NULL semantics for NOT IN). It runs before pushdown so that decorrelated subqueries become ordinary joins that the join graph sees.
3. `PushDownFilters`: through projections (substituting ids), aggregates (key-only conjuncts), windows (partition-key-only conjuncts), set operations, the preserved side of outer joins, and into CTE bodies when every reader agrees. It also infers equalities transitively (`a=b ∧ b=5 ⇒ a=5`). Runs as a single top-down walk, then once more after join order, as DuckDB does.
4. `ReorderJoins` (the only cost-based pass, see section 3): it replaces each inner-join graph with the chosen tree. (Amended in M3b: the binder binds a tree of inner joins as one unordered `InnerJoins` node with the planner's leaves, rather than the pass extracting maximal graphs from `Join` trees, which would merge joins inside parenthesized subqueries and change plans.)
5. `ShareSubplans`: merges uncorrelated scalar subqueries over structurally equal inputs (generalizes #2243 without SQL text) and decides CTE sharing by reference count.
6. `PruneColumns`: by id, over every node, as a logical pass. Passes match the closed enum without a wildcard default, and non-exhaustive-match warnings are made fatal for `wvlet-engine`, so a node kind that the pass does not handle fails the build (pain point 2). It runs across CTE boundaries using the union of the readers' needs (pain point 7).
7. `PushScanFilters`: attaches translatable conjuncts to `Scan.pushed` for row-group skipping, wherever they end up after pushdown. It no longer requires the filter to sit directly over the scan.
8. `Lowering` (physical choices): hash join build side by estimated rows (today's `SwapBuildRatio`), grouping sets on a shared input with constant keys dropped (#2238), TopN, and later late materialization.

Rule hygiene, applying the lessons of pain point 1:
- Every pass and rule has a name. `wvlet.engine.optimizer.disabled=<names>` turns it off, like DuckDB's `disabled_optimizers`.
- Each optimizer run records the rules that fired. Tests assert on this, which catches a rule that never fires, as in #2249.
- A `PlanValidator` runs between passes in tests and debug mode. It checks that every `ColumnRef` is produced below its use, that types are consistent, and that the output ids of the root are unchanged. This is the equivalent of Spark's plan-change validation.

### 3. Cost-based join order only

- `JoinGraph`: relations (any `LogicalPlan` subtree that is not an inner join), edges (equi and non-equi conjuncts with their id sets, computed once), and hyperedges for conditions that read more than two relations.
- Enumerator: **DPhyp** (or the simpler DPccp, since nearly all our edges are binary) when the graph has at most `exactThreshold` relations (proposal: 10), with a budget of 10,000 emitted pairs as in DuckDB. Otherwise, or when the budget runs out, use **today's multi-start greedy** (#2239), moved over unchanged. The greedy builds left-deep trees; DPhyp can also produce bushy ones. Lowering keeps left-deep probe pipelines where the cost is equal, because MorselOp parallelism likes long probe chains. The cost function should either include a pipeline-break penalty or the enumerator should be limited to left-deep trees; see decision D4.
- Cost: C_out (sum of intermediate rows) plus hash build rows. This is today's cost, so the move doesn't change it.
- Planning-time guard: keep q64 (about 18 inputs) under its current ~14 ms. Above the threshold, the greedy path is unchanged.

### 4. Statistics and cost model

- `Stats(rows: Double, cols: Map[ColumnId, ColStats(ndv, min, max, nullFrac)])`, derived bottom-up per `LogicalPlan` node and memoized by node identity. This replaces `estimateRows` / `columnDistinct` walking physical operators. Because estimates are derived through every node kind, Aggregate (rows ≈ min(rows, ∏ ndv(keys))), Union, Window, and CTE stop inheriting their first input's row count.
- Source stats come from `TableSource` (Parquet/Iceberg row counts, min/max, null counts, and cached distinct counts), as today.
- Selectivity: keep today's rules (1/ndv for `=` with a constant, n/ndv for an IN list, 0.3 otherwise), add range selectivity from min/max for integers and dates, and add null fractions for `is null`. These follow DuckDB's few-statistics stance: no histograms.
- **Adaptive (later milestone).** The engine already executes at plan time: scalar subqueries and `SharedResult`. Make that explicit: a materialized CTE or subquery becomes `Constant` with exact stats, and `ReorderJoins` runs again over the graphs that read it. Runtime join filters (min/max or IN-set of the build keys pushed into the probe scan's row-group filter, like DuckDB's join filter pushdown) are the other cheap adaptive win, since `ScanFilters` already exists.

### 5. Where rules live: engine for speed, compiler for meaning

- **Engine-only:** all speed rules (pushdown, join order, pruning, sharing, decorrelation strategy). DuckDB, Trino, and Snowflake already do these on the SQL we generate, and changing the generated SQL would make `wvlet compile` output harder to read.
- **Compiler:** meaning-level normalizations that every backend must agree on stay as compiler phases, as `RewriteExpr` does today. If an engine rewrite turns out to be meaning-level (for example how a Wvlet operator maps to NULL semantics), it moves to the compiler and gets a Lean model there.
- **Lean coverage.** The engine IR is small enough to model a core of it in Lean: bags of rows with SQL three-valued logic over `Filter`, `Project`, `Join(inner/left/semi/anti)`, and `Aggregate`. Rules worth proving, because they have NULL traps: filter pushdown through outer joins (null rejection), OR factoring under 3VL, NOT IN → anti join with NULLs, EXISTS/IN decorrelation, and dropping constant grouping keys. These follow the existing pattern (ADR 2026-11-18 Lean formal verification): Lean emits `(input plan, rewritten plan)` traces, and a `LogicalRuleConformanceTest` replays them against the Scala rules. Join enumeration is covered by a property test instead: every condition is placed exactly once, and every relation appears once.

### 6. Testing

- **Per-rule tests:** build a `LogicalPlan` from a small wvlet query (through the binder) and assert the plan after a single pass (`Optimizer.only(pass)`), using a stable text form of the logical plan with ids printed as `name#id`.
- **Golden plans:** `explain` output for all TPC-H and TPC-DS queries is checked in under `spec/engine-plans/` (TPC-DS run on sf=0.1 stats, so that it is cheap), in the style of Trino's cost-based plan tests. A PR that changes plans shows the change in its diff. CI fails on unexpected drift and prints the command that regenerates the files, as the stdlib freshness check does.
- **Rule toggles as differential tests:** run spec/basic with each pass disabled in turn. The results must not change; only the plans may. This finds rules that a correct result secretly depends on.
- **Conformance and benchmarks** stay the gate for every milestone: DuckDB conformance over spec/basic, TPC-H, and TPC-DS, plus `EngineTPCHBench` / `EngineTPCDSBench` medians compared before and after.

### 7. Migration path

Each milestone is one or two PRs, and each keeps conformance and the benchmarks.

- **M0: safety net, no behavior change.** Golden physical `explain` files for TPC-H and TPC-DS, an optimizer-disable flag wired to today's ad hoc spots (OR factoring, join reorder, scalar prefetch, pruning, row-group filters), and the differential toggle test. This is small and lands first. It immediately helps the sessions that are editing `Planner` now.
- **M1: extract join ordering.** Move `planJoinTree`'s graph building, estimators, and greedy search into `JoinGraph` / `JoinOrderOptimizer` / `CardinalityEstimator`, still over lowered leaves. Plans are identical (golden files prove it). Then add DPccp/DPhyp ≤ threshold as its own PR, with benchmark evidence (q64, q72, q85, q24 first).
- **M2: logical IR and binder for the core.** Add `LogicalPlan`, a binder for Scan, Filter, Project, Join, Aggregate, Sort, Limit, and Values, and Lowering. Any other relation becomes a `Legacy(relation)` leaf that the old `Planner` lowers, so coverage grows node by node (a strangler pattern). A property switches the path (`wvlet.engine.planner=logical|legacy`). The logical path becomes the default once both pass TPC-H/TPC-DS with no benchmark regression.
- **M3: move the logical rules,** one PR each: Normalize (OR factoring), PushDownFilters, ReorderJoins on the IR (the M1 code, now reading `Stats`), PruneColumns by id, PushScanFilters. Then Decorrelate (`Apply`), ShareSubplans (replacing the SQL-text grouping), CTE pushdown across boundaries, and grouping sets. This is a migration order, not the pipeline order: until Decorrelate moves, subqueries stay `Legacy` leaves that the old planner decorrelates, so pushdown and join order simply don't see into them. Each PR deletes the matching `Planner` code.
- **M4: the remaining surface forms** (pivot, unpivot, unnest, ASOF, window details, `Count` of groups) move into the binder. The `Legacy` leaf and the old `Planner` are then deleted.
- **M5: adaptive and new rules:** re-planning after materialization, runtime join filters, late materialization for TopN over wide scans, and the Lean models for the 3VL-sensitive rules. These can start once the rules they touch exist on the IR.

Until M2 switches the default, the parallel sessions keep editing `Planner` as now. From M3 on, a new rule goes into the IR.

## Alternatives and Why Not?

- **Keep growing the current `Planner` and add a few more physical passes like `ColumnPruner`.** This is the cheapest option in the short term. But pain points 2–5 are structural: without column identities and a logical stage, every rule keeps re-resolving names, matching text, and depending on case order. With three sessions editing the same 2.8k-line file, the cost is already visible.
- **Optimize at the compiler `Relation` level, as compiler phases.** This reuses `RewriteRule`, and the SQL generators could benefit. But `Relation` has about 50 node types, many of them surface syntax, and it has names, not ids, so each rule would handle the surface forms and resolve names again. Target engines already optimize the generated SQL, and readable `wvlet compile` output matters more to us than pre-optimized SQL.
- **A Trino-style IterativeOptimizer with a memo and a pattern library.** Its benefits are local rules and a per-rule tester. We get both from partial-function rules in batches. The memo, `Lookup`, and pattern combinators add code that Scala.js and Native pay for, and the fixpoint across hundreds of rules is where Trino's hard-to-predict interactions come from.
- **Cascades or Volcano (Calcite VolcanoPlanner, Optgen exploration).** These are fully general, but we have one real search space (join order), which a dedicated enumerator handles better and with a predictable planning time. The risks are large code, tuning, and search blowup.
- **E-graphs (egg).** They have no phase ordering, but they blow up on join reordering and need a cost-guided extractor. They are research-grade for relational plans. We may look at them for scalar-expression simplification only, much later.
- **Rewriting the physical `Operator` tree only.** Positions instead of ids make every rule remap indexes (as `ColumnPruner` does), and physical operators have already fixed join order and build sides.
- **Bushy DP for every join graph size.** Planning time grows exponentially with graph size; DuckDB, Spark, and Trino all cap exact search (12, 12, and 8 relations) and fall back to greedy.

## Decisions (accepted 2026-11-22)

All accepted as proposed:

- **D1. Rule mechanism:** fixed, ordered passes; each is a tree walk or a small fixpoint batch capped at 10 iterations.
- **D2. Where speed rules live:** in the engine only. Meaning-level rules stay in compiler phases, and the generated SQL is unchanged.
- **D3. IR:** a new engine logical IR with `ColumnId`s, migrated behind a `Legacy(relation)` leaf (M2–M4).
- **D4. Join enumerator:** DPhyp/DPccp for up to 10 relations with a 10,000-pair budget and a greedy fallback. Bushy trees are allowed; the cost adds a pipeline-break term so that left-deep probe chains win ties.
- **D5. Lean:** model the 3VL-sensitive rules (pushdown through outer joins, NOT IN, OR factoring, decorrelation) with trace conformance (M5).
- **D6. Golden plans:** golden TPC-H/TPC-DS plan files gate CI, with a regenerate command (M0).
- **D7. Adaptive execution:** in scope (M5).
