# ADR: A pass-based optimizer with cost-based join order for the wvlet engine

Date: 2026-11-22 · PR #2254

Status: accepted; implementation in progress (milestones M0–M5 in the plan).

## Context

`wvlet-engine` optimizes inside its `Planner`. The planner binds names, rewrites, and builds physical operators in one recursion over the compiler's `Relation`. After that, two physical passes run: `ColumnPruner`, which also attaches Parquet row-group filters, and `Parallelizer`. The optimizations so far are OR factoring, join-graph flattening, filter pushdown, greedy join order from distinct-count estimates, decorrelation of EXISTS/IN/scalar subqueries, shared CTEs and shared scalar-subquery inputs, grouping sets on a shared input, and column and row-group pruning. Each was added where it happened to fit.

The history shows what this costs:

- A rewrite case that never matched because a generic case came first (#2249).
- Pruning that stopped silently below windows and set operations (#2222, #2234), and that is dropped for the whole plan when one expression can't be remapped.
- Analysis done by trial binding and catching `ColumnNotFound`, which made planning q64 take 293 ms until #2239 cached it.
- Expressions and subplans compared by printed SQL, which is why #2243 has to skip every input that might read a CTE.

There is no logical stage, so a rule that needs a decision made later in the recursion, or a filter that should cross a CTE boundary, has nowhere to go.

DuckDB, Trino, Spark, Calcite, CockroachDB, and DataFusion all optimize a bound logical IR in which columns have stable ids. All of them limit cost-based search to join order and physical choices. The engine also builds for Scala.js and Scala Native, where code size matters.

## Decision

Accepted as proposed on 2026-11-22. See `plans/2026-11-22-engine-optimizer-framework.md`.

- Add an engine-local logical IR (`LogicalPlan`): a closed enum of about 16 nodes whose columns are `ColumnId`s. Names are bound once, Relation → LogicalPlan, and surface operators are normalized into core nodes while binding. Lowering then maps ids to positions and builds `Operator`s, and `Parallelizer` stays a physical pass.
- The optimizer is an explicit, ordered list of named passes, as in DuckDB and Calcite's HepPlanner: Normalize, Decorrelate, PushDownFilters, ReorderJoins, ShareSubplans, PruneColumns, PushScanFilters. A pass is a hand-written traversal or a small batch of partial-function rules run to a fixpoint with an iteration cap. There is no memo, Cascades search, or rule DSL.
- Amended in M3b (#2268): `ReorderJoins` does not extract maximal inner-join graphs from `Join` trees. The binder binds each tree of inner joins as one unordered `RuntimePlan.InnerJoins` node with the planner's leaves, and the pass replaces it. Extracting from `Join` trees would merge joins inside parenthesized subqueries into the outer graph and change plans. Until M4 the greedy search is one generic copy (`JoinSearch`) shared with the legacy planner, so the two planners choose the same order.
- Join order is the only cost-based search. It uses DPhyp/DPccp up to 10 relations with a 10,000-pair budget (bushy trees allowed, with a pipeline-break term in the cost), and falls back to today's multi-start greedy. The cost is C_out plus hash build rows, and statistics are derived per logical node from Parquet/Iceberg metadata.
- Speed rules live in the engine; meaning-level normalizations stay in compiler phases, as `RewriteExpr` does. The SQL generated for DuckDB, Trino, and Snowflake does not change.
- Every pass can be disabled by name, records when it fires, and is checked by a plan validator between passes. Golden `explain` files for TPC-H and TPC-DS make plan changes visible in PRs.
- Migration goes in steps: a safety net first, then extracting join ordering, then the IR behind a `Legacy(relation)` leaf that grows node by node, then the rules moved one by one, and finally adaptive re-planning. Each step keeps TPC-H/TPC-DS conformance and the benchmarks.

## Consequences

- New rules go into one place with a known order, and they compare expressions by id rather than by text. A pass that misses a node kind no longer slows a query without warning: passes match the closed enum without a wildcard default and the engine build makes non-exhaustive-match warnings fatal, so the missing case fails the build.
- Migration costs: the binder and Lowering duplicate parts of `Planner` until the legacy path is deleted (M4), and sessions editing `Planner` in the meantime will have to move their changes over.
- Join planning time stays bounded: graphs larger than the threshold use the current greedy path.
- Rules whose correctness depends on SQL's three-valued logic (pushdown through outer joins, NOT IN, OR factoring, decorrelation) become small enough to model in Lean with trace conformance, following the 2026-11-18 Lean ADR.
- Rejected options, with reasons in the plan: growing the current planner, optimizing at the compiler `Relation` level, a Trino-style memo with a pattern library, Cascades/Volcano, e-graphs, rewriting physical operators only, and unbounded bushy DP.
