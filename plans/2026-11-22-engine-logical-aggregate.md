# Engine optimizer M2b, PR 2: aggregations in the logical IR

Part of milestone M2 (`plans/2026-11-22-engine-optimizer-m2-logical-ir.md`), after joins (`plans/2026-11-22-engine-logical-join.md`). The default-planner switch follows.

## Goals

- GROUP BY, HAVING, global aggregations, and aggregating projections become logical nodes with column ids, so that M3 can push filters through aggregations (key-only conjuncts) and prune their columns by id.
- No plan changes: both planners still match every golden TPC-H and TPC-DS plan.

## Background

- `planAggregation` plans the input, binds grouping keys, collects aggregate calls while binding the select items (`AggContext`), binds HAVING, then builds `HashAggOp` → `FilterOp` → `ProjectOp`. Binding order matters: it numbers aggregate calls and hidden names.
- Grouping sets and window functions over aggregated rows build operator trees of their own (shared input readers, a window collector over the aggregation's operator).

## Design

Lane: Express. An internal refactor behind `wvlet.engine.planner=logical`, whose default stays `legacy`; the golden plans prove nothing changes.

- Add `Aggregate(child, keys, calls, columns)`: output is the keys, then one column per call. Calls keep the executable `AggCall` shape, with `BExpr` arguments and filters over the input's ids. Lowering emits the same `HashAggOp`.
- `planAggregation` becomes generic over what it builds, as `planJoinTree` did: the planner builds operators, and the binder builds logical `Aggregate`, `Filter` (HAVING), and `Project` nodes paired with their operators. It is the same code, so binding order and planner side effects are unchanged.
- The binder routes `GroupBy`, `Agg`, `Count`, an aggregating `Project`, and HAVING without a selection to it, in the planner's case order.
- These stay `Legacy`: pivot and unpivot, `Count` of groups, and `Distinct`. The planner builds grouping sets over the bound input, and they come back as a `Legacy` node under the logical HAVING and projection (grouping sets are M3 work). With windows over aggregates, the whole result is a `Legacy` node over the bound input (M4).
- Tests: the golden plans on both planners, spec/basic with the validator, and binder tests for an aggregation over a join, HAVING as a filter over the aggregation, and grouping sets as a `Legacy` node.

## Alternatives and Why Not?

- A separate logical aggregation binder: it would duplicate the key, alias, `select *`, and `arbitrary(...)` rules of `planAggregation`, and binding order would drift from the planner's.
