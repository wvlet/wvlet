# Engine optimizer M3c, PR 4: set operations on the runtime plan, with filter pushdown into them

## Goals

- Bind UNION (ALL and distinct), Concat, INTERSECT, EXCEPT, and DISTINCT/dedup as runtime-plan nodes instead of `Legacy` leaves, so the passes see through them.
- Push filter conjuncts into the inputs of set operations and below DISTINCT (framework pass 3, "set operations").
- Keep the golden plans unless a change is justified, with benchmark evidence.

## Background

- The legacy planner plans a UNION as `UnionAllOp` over both inputs (plus `DistinctOp` for UNION), with the output types the common type of the inputs at each position and the names of the first input; INTERSECT/EXCEPT are `SetOpOp` with the same schema rule. Values are cast to the output types inside the operators.
- On the runtime planner these are `Legacy` leaves today, so a filter over a UNION stays above it, and an inner join tree over a UNION input can't push its single-input conditions into the branches.
- The physical `ColumnPruner` already prunes through `UnionAllOp` by position and keeps every column of `SetOpOp`/`DistinctOp`.
- Windows and CTEs read once (the other two parts of this item entry) need their own binding work (window calls, inlining CTE bodies on the IR) and change different plans, so they stay separate PRs.

## Design

Lane: Express. Internal planner change with no user-facing behavior change; the direction and gates were given in the launch prompt.

- **IR nodes.** `Union(inputs, columns)` (concatenation with casts to the output types), `SetOp(left, right, intersect, all, columns)`, and `Distinct(child)` (same columns as its child). UNION distinct binds as `Distinct(Union(...))`, as the planner builds it. Output columns follow the planner's `mergeSchemas` rule, so the lowered operators are the legacy ones.
- **Binding.** The binder routes these relations at the planner's case positions and binds each input once (the planner plans UNION inputs twice, once only for the schema; the second planning has no effect on the plan).
- **PushDownFilters.** A deterministic conjunct over a `Union` or `SetOp` output is rewritten per input, by position, with the input column cast to the output type when they differ, and pushed into every input (as DuckDB does for UNION, INTERSECT, and EXCEPT: a row removed by the filter on one side has an equal row filtered on the other). It passes a `Distinct` unchanged. An input with fewer columns than the output keeps the conjunct above.
- **Other passes.** `Stats` reads rows through the first input and no distinct counts, as `CardinalityEstimator` does over these operators, so join order doesn't change. `PruneColumns` keeps every input column (physical pruning still narrows union inputs by position). `PlanValidator` checks input widths and that output ids are fresh.
- **Golden plans.** Expected to change only where a filter or a single-input join condition sits over a set operation; any change ships with benchmark evidence (A/B against `WVLET_ENGINE_PLANNER=legacy`, which keeps today's plans).
- **Tests.** `Optimizer.only(PushDownFilters)` cases for UNION ALL with casts, UNION distinct, INTERSECT, EXCEPT, and DISTINCT; conformance on both planners, golden plans, and the toggle test.

## Alternatives and Why Not?

- **One PR for CTEs, windows, and set operations.** Each needs new binding and touches different golden plans; three reviewable PRs are easier to benchmark one by one.
- **Push only into the left input of EXCEPT.** Correct too, but leaves the right input unfiltered for no benefit, since rows equal under the set comparison give the conjunct the same value.
- **Prune union inputs by id in PruneColumns now.** The physical pruner already does it; moving it waits for the logical pass to narrow leaves (PushScanFilters).
