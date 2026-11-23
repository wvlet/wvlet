# M4b, PR 1: bind column edits, Describe, and EmptyRelation on the runtime plan

## Goals

- Start M4b: the surface forms that still reach `Planner.planLegacy` from `RuntimeBinder` bind as runtime-plan nodes, so that the runtime passes (PushDownFilters, PruneColumns, ReorderJoins) see through them and the `Legacy` leaf can eventually go.
- This PR covers the first entry of the M4b list.

## Background

- Of the first entry, Distinct and Dedup already bind (`RuntimePlan.Distinct`, since #2274), and so do set operations. What remains are the column edits (`add`, `prepend`, `exclude`, `rename`, `shift`), `describe`, and `EmptyRelation` (a query without `from`).
- The planner plans each column edit as a `ProjectOp` over its input (with a `WindowOp` first when new columns call window functions), `describe` as a batch of (name, type) rows from the child's schema, and `EmptyRelation` as a `SingleRowOp`.
- No TPC-H or TPC-DS spec uses these forms, so golden plans and benchmarks are not affected.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- Share the planner's logic: the column-selection rules (replace-in-place for `add` of an existing name, exclude/shift positions, rename schema, describe rows) move into small `private[engine]` planner helpers over a `Schema`, used by both the planner and the binder.
- Column edits bind as `RuntimePlan.Project` over the bound input; `add`/`prepend` with window functions bind a `RuntimePlan.Window` under the projection, as window projections already do.
- `add`/`prepend` whose new columns hold aggregates, unnest, or subqueries stay on the legacy path, matching the existing `Project` routing.
- `describe` binds its child for its columns only and becomes `RuntimePlan.Values`; `EmptyRelation` becomes a one-row `Values` without columns, which lowers to `SingleRowOp` as before.
- Tests: EngineRuntimePlanTest checks the bound shapes, no legacy leaves, the plan validator, and results equal to the legacy planner.

## Alternatives and Why Not?

- A dedicated `RuntimePlan.ColumnEdit` node: a projection already expresses every edit, and the passes handle `Project`.
- Lowering `describe` and `EmptyRelation` through a new node: `Values` already holds plan-time rows.
