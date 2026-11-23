# M4b, PR 9: bind sort keys with subqueries on the runtime plan

## Goals

- Continue M4b with its "sort keys with subqueries" entry: a sort whose keys hold a subquery binds as a runtime-plan `Sort` instead of a `Legacy` leaf over `Planner.planSort`.

## Background

- `RuntimeBinder.sort` sent any sort with a subquery in a key to `planner.planSort` as a `Legacy` leaf. That covered the sort and its whole input, so the passes could not see through it.
- `Planner.planSort` binds sort keys through `Binder.bind`, which evaluates uncorrelated subqueries (scalar, `[NOT] IN`, `EXISTS`) as plan-time constants. These are the same calls that select items use, and #2293 bound those on the runtime plan.
- `RuntimeBinder.sort` already mirrors `planSort` for keys without subqueries: keys over the input, or keys outside the select list sorted as hidden select items. Since #2293, a projection with subquery items binds without legacy leaves, so the hidden-key path also covers subquery keys.
- Correlated sort-key subqueries fail in the `Binder` on both planners alike.

## Design

- Lane: Express. This is an internal refactor with no change in results, and the M4b item pre-approves it.
- Drop the subquery exclusion from `RuntimeBinder.sort`. Subquery keys bind as constants in `RuntimePlan.Sort`, or through the hidden projected key when the key reads a column outside the select list, exactly as `planSort` binds them.
- No new RuntimePlan node. The lowered operators are the planner's (`SortOp`, `ProjectOp`), so golden plans stay unchanged (verified) and Stats need no change.
- The legacy-leaf test in EngineRuntimePlanTest moves to a filter with an `IN` subquery when Decorrelate is off, the next entry on the item.
- Tests: EngineRuntimePlanTest binds a scalar-subquery key, an `IN (subquery)` key with a top-n limit, and a subquery key outside the select list. Each plan has no legacy leaves, passes the plan validator, and returns the legacy planner's results.

## Alternatives and Why Not?

- Always sorting on a hidden projected key: this adds a projection even when the constant key binds directly over the input, and the planner does not do it.
