# M4b, PR 3: bind Unnest on the runtime plan

## Goals

- Next entry of the M4b list (Pivot, Unpivot, Unnest), split by form: this PR binds unnest. Pivot and Unpivot follow in their own PR.
- Unnest's three forms bind as runtime-plan nodes instead of `Legacy` leaves, so the runtime passes see through them: `from unnest(list)`, a lateral join with unnest (`cross join` / `join ... on` / `left join ... on true`), and `select ..., unnest(list), ...`.

## Background

- The planner builds one `UnnestOp` for all three forms: over a `SingleRowOp` for the relation form, over the left input (keeping its columns) for a lateral join, and between two `ProjectOp`s for the select-item form. A lateral join applies its ON conditions and the filters above it as conjuncts over the `UnnestOp`.
- Inside an inner-join tree, a join with a lateral unnest is one leaf (`flattenJoins`), so it reaches the binder's join path on its own.
- Pivot and Unpivot run their input at plan time (`Operator.materialize`), for the pivot values and the unpivot copies, so they need a different design.
- Only spec/basic uses unnest (`unnest.wv`, `unnest-cross-join.wv`, `unnest-select-star.wv`, `stdlib_range.wv`). No TPC-H or TPC-DS spec does, so golden plans and benchmarks are not affected.

## Design

- Lane: Express. This is an internal refactor with no change in results, and the M4b item already approves it.
- New node `RuntimePlan.Unnest(child, lists, keepInput, outer, columns)`. Its output is the child's columns (with `keepInput`), then one element column per list. It lowers to the same `UnnestOp`.
- Both planners use the same planner helpers to bind the lists and element fields, to check a lateral join (outer for `left join ... on true`, the errors for other forms), and to split select items. That way both build the same operators.
- Relation form: `Unnest` over the one-row `Values`, which lowers to `SingleRowOp` as before.
- Lateral join: `Unnest` over the bound left input. Conditions without subqueries bind as a `Filter` over it. Conditions with subqueries go through `plans.conjuncts` as other filters do.
- Select-item form: `Project` → `Unnest` → `Project`, as the planner builds it. A projection whose items also hold window calls or subqueries stays legacy.
- PushDownFilters moves a conjunct that reads only the child's columns below `Unnest`. Every output row repeats its input row's columns, so the filter keeps the same rows, including rows that `outer` keeps and NULL values. Conjuncts that read element columns stay above.
- PruneColumns narrows the child to the needed columns plus the lists' columns. Every element column is kept, since dropping a list changes the row count.
- ShareSubplans canonicalizes the node, since it is deterministic. Stats: rows pass through and element columns have no distinct counts, as CardinalityEstimator does for `UnnestOp`.
- Tests: EngineRuntimePlanTest checks the bound shapes, a filter moving below a lateral unnest, the plan validator, and results equal to the legacy planner. The spec/basic unnest specs run on both planners.

## Alternatives and Why Not?

- Lateral unnest as a `Join` with a correlated right input: the IR has no correlation, and every join pass would need to learn it.
- Keep the select-item form legacy and bind only the relation and join forms: the projection form shares the same node, and keeping it would leave a Legacy leaf for a common SQL form.
