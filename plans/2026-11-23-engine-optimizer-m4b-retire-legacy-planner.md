# Engine optimizer M4b: retire the legacy planner

## Goals

- Finish M4b: after this PR the engine has one planning path, `RuntimeBinder` → `RuntimePlan` → `Lowering`
- Delete what only the legacy planner used, and keep what `RuntimeBinder` still calls

## Background

- Since #2298 the runtime plan has no `Legacy` node, so `planRelation`'s `PlannerMode.Legacy` branch is the only caller of `Planner.planLegacy`
- Tests still compared the runtime engine's rows with a `legacy` engine: EngineRuntimePlanTest, EngineDecorrelateTest (also its `noDecorrelate` checks), EngineGroupingSetsTest (both planners against DuckDB), EngineBasicSpecLegacyTest, and EnginePlanGoldenTest (`spec/engine-plans/legacy/`)

## Design

Lane: Express. An internal refactor of the engine: results and golden plans stay the same, and the default planner was already the runtime one.

- `Planner.planRelation` always binds, optimizes, and lowers. Delete `planLegacy`, along with every Planner member that only it reached. A reachability pass over Planner.scala's members, starting from references outside the file, found them: the operator-level `plan*` helpers (table, values, aliased, filter, sort, project, column edit, aggregation, join tree, outer and ASOF joins, unnest, pivot/unpivot, sample, SQL select), `applyConjuncts` and the operator `bindWithSubqueries` / `existsJoin` / `inJoin` / `semiJoin` / `rewriteSubqueryConjunct`, `operatorPlans`, `joinBinding`, the eager shared-CTE path (`cteTables`), and small helpers (`requalify`, `projectIndexes`, `dropHidden`, `unionSchema`, `scanOp`, window/unnest projection helpers). Planner.scala goes from 3,357 to about 2,630 lines
  - Kept: everything generic over a plan builder (`aggregation`, `pivot`, `unpivot`, `usingJoin`, `joinOn`, `correlatedJoin`, `decorrelateScalar`, the generic `bindWithSubqueries`), and the helpers `RuntimeBinder` calls (`desugarSQLSelect`, `describeRows`, `excludeIndexes`, `renameSchema`, `shiftIndexes`, `asOfJoinSpec`, `sampleFilter`, ...)
- Delete `PlannerMode`, `OptimizerOptions.planner` / `withPlanner` / `parsePlanner`, and the `wvlet.engine.planner` / `WVLET_ENGINE_PLANNER` switch. A leftover setting is ignored, not rejected: no value has a meaning now, and only tests and benchmarks set it
- Delete `spec/engine-plans/legacy/` and its handling in EnginePlanGolden / EnginePlanGoldenTest, and EngineBasicSpecLegacyTest
- Tests compare with DuckDB through generated SQL, as decided in the retire-legacy-leaf note. `EngineConformance.compare` is now public and takes the engine to run and an `ordered` flag (in-order comparison where ORDER BY keys are a total order); `assertMatches` fails on a mismatch or a DuckDB error, and `EngineConformance.tpch` builds the TPC-H setup both tests share. EngineRuntimePlanTest and EngineDecorrelateTest build their engine from an `EngineConformance` over the TPC-H views and fail when DuckDB rejects the SQL (otherwise the comparison would be skipped). EngineGroupingSetsTest runs once
  - Forms DuckDB evaluates differently get fixed rows: `sample N` (DuckDB draws other rows: row count there, the first 5 nations here), `sample 10%` (the engine's seeded draw: 4,886 rows), `describe` (DuckDB returns no rows for the generated SQL), a pivot without listed values (DuckDB's PIVOT also groups by the other columns), and a column-edit chain ending in `order by` a renamed column (the generated SQL orders by a name DuckDB can't resolve: a GenSQL gap, outside this PR)
  - Error tests drop their legacy half
- `RuntimeBinder`'s `Lowered.op` stays. The shared builders read `b.op(p)` for schemas, and `JoinOrderOptimizer` reads it for `CardinalityEstimator` estimates. Removing it means giving `JoinPlanBuilder` schema and `Stats` accessors in place of the operator, which touches the join search. Recorded as a follow-up
- Also a follow-up: `JoinBinding`, `OperatorTreeBuilder`, and `JoinOrderOptimizer.expressionSearch` lost their production caller (the planner's operator join trees) and remain only for JoinOrderOptimizerTest. They are in the join enumerator, which this PR stays out of
- Update CLAUDE.md, the golden-plans-per-planner and strangler ADRs (with amendments), the other 2026-11-22 engine ADRs that name the legacy planner, and the memories that used the legacy baseline
- Gates: EngineBasicSpecTest, TPC-H 22/22, TPC-DS 99/99, golden plans unchanged, the toggle, decorrelate, and runtime-plan tests, engineJS/Native compile. The runtime path's code is unchanged, so the plans and benchmark numbers should stay the same

## Alternatives and Why Not?

- Reject `wvlet.engine.planner` with an error: there's no value left to accept, so an error only breaks old scripts. Ignoring it costs nothing
- Fixed expected rows everywhere: these only pin today's output, while DuckDB is an independent check. Fixed rows are only for forms DuckDB evaluates differently
- Drop `Lowered.op` here: it changes the join-order estimates' input type, which this deletion doesn't need. Better as its own PR
