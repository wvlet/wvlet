# Bind `count` of groups into the engine's runtime plan

## Goals

- First step of optimizer milestone M4: shrink what the runtime binder leaves as `Legacy` leaves, one surface form per PR, so that the legacy planner can be deleted once M3b lands.
- `from t group by k count` (count the groups) binds to runtime-plan nodes, so later passes (pruning, pushdown) see through it.

## Background

- The binder sends `Count` over an aggregation input to `planLegacy`, which plans the grouping and then a global `count_star` `HashAggOp` over it.
- The binder already binds groupings through `planner.aggregation` with the runtime-plan builder; only the outer count was missing.

## Design

- Lane: Express — internal refactor with identical physical plans; no always-gate area.
- `Count` of groups binds as `RuntimePlan.Aggregate(no keys, count_star)` over the bound grouping. Lowering produces the same `HashAggOp` as the planner, so golden plans don't change.
- The result column name follows the planner (the first select item's name, or `count`).
- Tests: the runtime plan of a group count is an `Aggregate` over an `Aggregate`, validates, and returns the same rows as the legacy planner.
- The other forms (pivot, unpivot, unnest, ASOF, windows, distinct, column edits, set operations, samples) follow in their own PRs.
