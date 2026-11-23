# M4b, PR 6: bind ASOF joins on the runtime plan

## Goals

- Next entry of the M4b list: bind ASOF joins in `RuntimeBinder` as runtime-plan nodes instead of `Legacy` leaves.
- Results stay identical on both planners, including NULL keys and values, which never match.

## Background

- `RuntimeBinder.nonLateralJoin` sent every `j.asof` join to `planner.planJoinTree` as a `Legacy` leaf, the last join form that did.
- `Planner.planAsOfJoin` splits the ON condition into equalities between the inputs (keys) and exactly one comparison between them (`>=`, `>`, `<=`, `<`, flipped when written right side first), then builds `AsOfJoinOp`, an inner or left join that matches each left row to the closest right row.
- No operator other than a hash join can express "the closest row", so the runtime plan needs a new node.
- The legacy planner applies filters over an ASOF join above it, never on either input.
- No TPC-H or TPC-DS query uses ASOF, so golden plans and benchmarks are unaffected; `spec/basic/join-asof.wv` and `table_val_asof_join.wv` cover it.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- A new `RuntimePlan.AsOfJoin` node (left, right, kind, keys, the two compared values, and the operator), lowered to the same `AsOfJoinOp`; every pass handles it.
- The ON-condition analysis moves out of `planAsOfJoin` into a shared `Planner.asOfJoinSpec`, which both planners use, so they build the same keys, casts, and operator.
- Filters over the join bind above it. PushDownFilters then moves conjuncts over left columns only below it: each left row matches the same right row whatever the other left rows are. Conjuncts over right columns stay above, since below they would change which right row is the closest.
- Stats follow CardinalityEstimator for `AsOfJoinOp`: the rows of the left input, and no traced distinct counts.
- PruneColumns keeps the keys and compared values on each side; ShareSubplans canonicalizes the node, since it is deterministic.
- The legacy-leaf test in EngineRuntimePlanTest moves to a projection with a scalar subquery, which still binds as a legacy leaf.
- Tests: EngineRuntimePlanTest checks the bound shape (no `Legacy` leaf), that a left-side filter moves below the join while a right-side one stays above, and that inner and left ASOF joins with each operator, NULL keys, and NULL values return the same rows as the legacy planner.

## Alternatives and Why Not?

- Reusing `RuntimePlan.Join` with an ASOF flag: every pass that reads join keys, residuals, or build sides (ReorderJoins, PushDownFilters' outer-join key extraction, Stats) would need to special-case it.
- Keeping filters exactly where the legacy planner puts them (all above): pushing left-only conjuncts is safe and lets them reach scans.
