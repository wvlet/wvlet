# M4b, PR 7: bind window and plain projections with subqueries on the runtime plan

## Goals

- Continue M4b with its "window evaluation in projections" entry: the projection forms that still reach `Planner.planLegacy` from `RuntimeBinder` bind as runtime-plan nodes.

## Background

- Window projections without subqueries bind as `RuntimePlan.Window` under a `Project` since #2287, and windows next to aggregates bind through the shared `Planner.aggregation` over `AggPlanBuilder`.
- What remains are projections, window projections, unnest projections, and column edits whose items hold subqueries. `isPlainItem` and `isUnnestProjection` excluded them, so the whole projection and its input became a `Legacy` leaf.
- In select items the planner evaluates subqueries while binding: a scalar subquery (`bindScalarSubquery`), `[NOT] IN (subquery)` (`bindInSubquery`, `bindTupleInSubquery`), and `EXISTS` (`bindExists`) become plan-time constants, through the same `Binder` that `planner.bindItems` uses. Correlated select-item subqueries fail on both planners alike.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- Drop the subquery exclusion from `isPlainItem` and `isUnnestProjection`: these items bind as constants in `Project`, `Window` + `Project`, `Unnest` projections, and column edits, exactly as the planner binds them. The input now binds on the runtime path, so the passes see through it.
- No new RuntimePlan node; the lowered operators are the planner's (`ProjectOp`, `WindowOp`, `UnnestOp`), so golden plans stay unchanged (verified) and Stats need no change.
- Still legacy: sort keys with subqueries, column edits with aggregates or unnest, unnest projections with windows (which fail on the planner too), and filters with subqueries when Decorrelate is off. The legacy-leaf test in EngineRuntimePlanTest moves to a sort key with a scalar subquery.
- Tests: EngineRuntimePlanTest binds a scalar subquery next to a window, `IN (subquery)` in a select item, a column edit with a subquery and a window, and an unnest projection with a subquery, with no legacy leaves, a clean plan validator, and results equal to the legacy planner.

## Alternatives and Why Not?

- Decorrelating select-item subqueries into joins: the planner doesn't, and constants are cheaper for uncorrelated ones; correlated select-item subqueries are a separate feature.
