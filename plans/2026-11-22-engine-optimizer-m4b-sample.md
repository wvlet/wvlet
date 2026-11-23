# M4b, PR 2: bind Sample on the runtime plan

## Goals

- Next entry of the M4b list: `sample` binds as runtime-plan nodes instead of a `Legacy` leaf, so the runtime passes see through it.
- Set operations (Union, Concat, Intersect, Except) already bind as `RuntimePlan.Union` / `SetOp`; verified in `RuntimeBinder.bind`, so they are not part of this PR.

## Background

- `Planner.planSample` plans `sample N` (rows) as `LimitOp(op, N, 0)`, `sample P%` as a `FilterOp` with a `RowFn("sample")` drawing from `Random(0)`, and a percentage given by an expression as its input unchanged. The method (`reservoir`, `system`, `bernoulli`) is ignored.
- Only `spec/basic/sample.wv` uses `sample`; no TPC-H or TPC-DS spec does, so golden plans and benchmarks are not affected.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- `sample N` binds as `RuntimePlan.Limit(child, N, 0)`; a percentage expression binds as the child alone, as the planner plans them.
- `sample P%` binds as a new `RuntimePlan.Sample(child, percentage)`, lowered through a shared `Planner.sampleFilter` to the same seeded `FilterOp`, so both planners build the same operator. The generator is created at lowering, so each lowering samples from a fresh seed.
- The node is a barrier: PushDownFilters keeps filters above it (moving a filter below changes which rows draw which random numbers), and ShareSubplans does not share it, as it does not share legacy leaves. PruneColumns narrows below it, which leaves rows and their order unchanged.
- Stats: rows and column distinct counts pass through, as CardinalityEstimator does for the `FilterOp` it lowers to.
- Tests: EngineRuntimePlanTest checks the bound shapes, that a filter stays above the sample, the plan validator, and results equal to the legacy planner. The legacy-leaf test now uses `pivot`.

## Alternatives and Why Not?

- A `RuntimePlan.Filter` with the `sample` RowFn, marked non-deterministic in PushDownFilters: the filter would carry one generator built at bind time, shared by every lowering of the plan, and other passes reading filters (join conditions, shared-subplan keys by expression signature) would each need to know about it.
