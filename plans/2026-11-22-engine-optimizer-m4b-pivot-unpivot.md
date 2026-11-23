# M4b, PR 4: bind Pivot and Unpivot on the runtime plan

## Goals

- Next entry of the M4b list: `pivot` and `unpivot` bind as runtime-plan nodes instead of `Legacy` leaves, so the runtime passes (filter pushdown, column pruning) see through a pivot to its input, and into each unpivot branch.
- Results stay identical on both planners (row order, as before, only under `order by`).

## Background

- `Planner.planPivot` materializes its input at plan time (`Operator.materialize`), reads the distinct non-NULL values of each pivot column from it when the query lists none, and plans a `HashAggOp` over the materialized batch with one aggregate per (value combination, aggregate), each filtered by `col = value`. It materializes even when every value is listed.
- `Planner.planUnpivot` materializes its input and plans a `UnionAllOp` with one branch per target column: `Project(Filter(copy of the batch, target is not null), other columns, 'name', cast(target))`.
- Grouping sets already solve "one input, several readers" for both planners: `AggPlanBuilder.sharedInput` gives readers of one lazily computed result (a `CTERef` over a `SharedInput` on the runtime plan, a `SharedResult` reader on the legacy one), and `AggPlanBuilder` builds `union`, `aggregate`, `filter`, and `project` for either planner. It gains one method, `values`, for rows computed at plan time.
- Only spec/basic (`pivot*.wv`, `unpivot.wv`) uses these forms; no TPC-H or TPC-DS query does, so golden plans and benchmarks are unaffected.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- Pivot and Unpivot move into shared planner helpers over `AggPlanBuilder[P]`, so `Planner.planLegacy` and `RuntimeBinder` build the same operators from one piece of code, as grouping sets do. No new `RuntimePlan` node is needed.
- Pivot binds as an `Aggregate` with filtered calls over its input. It materializes the input only when some pivot key lists no values: then the values come from the batch, and the aggregate reads the batch (a `Values` node on the runtime plan, `BatchesOp` on the legacy one), so the input is computed once, as today. With every value listed, the aggregate reads the input directly, and nothing runs at plan time.
- Unpivot binds as a `Union` of `Project(Filter(reader, target is not null))` branches over one `sharedInput` of its input. The input is computed once, on first read, instead of at plan time. With one target column the branch reads the input directly, without a shared input.
- The legacy planner changes in the same way (no plan-time materialization for listed pivot values or for unpivot). Its operators differ only in where the input comes from; rows are unchanged.
- Filters above an unpivot push into each branch over the shared reader, as for grouping sets; nothing moves below the shared input (`SharedInput` ignores reader filters, and PruneColumns stops at `CTERef`), so a filter on a non-target column runs once per branch. Acceptable for this form; pushing into shared inputs is the CTE-sharing work of M3b.
- Stats need no code change and still agree with CardinalityEstimator, but the estimates move: an unpivot over a shared input is estimated as a shared result (1e6 rows) instead of a `BatchesOp` (1e3), and a pivot with listed values takes its input's estimate. Only spec/basic uses these forms, so no golden plan changes.
- Tests: EngineRuntimePlanTest checks the bound shapes (no `Legacy` leaf for either form, with and without listed values), the plan validator, and results equal to the legacy planner; the legacy-leaf test moves to a form that is still legacy. spec/basic runs on both planners.

## Alternatives and Why Not?

- New `RuntimePlan.Pivot` / `Unpivot` nodes: every pass would need a case, though both forms are expressible with existing nodes.
- Unpivot branches each re-binding the input: computes the input once per target column.
- Pivot always reading a materialized `Values`: keeps plan-time execution for queries that list their values, and hides the input from pruning and pushdown.
