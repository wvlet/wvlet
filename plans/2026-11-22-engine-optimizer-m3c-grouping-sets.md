# M3c: grouping sets on the runtime plan

## Goals

- Bind GROUP BY ROLLUP / CUBE / GROUPING SETS aggregations as runtime-plan nodes instead of a `Legacy` leaf, the last M3c entry, so the logical passes (PushDownFilters, PruneColumns above the sets) see through them and M4 has one less operator-only form
- Lower to the operators the legacy planner builds today (#2238): one shared, column-reduced evaluation of the input, one hash aggregation per set over its non-constant keys, a projection putting the NULL keys and grouping id back, and a union

## Background

- `Planner.aggregation` is already generic over `AggPlanBuilder`; only the grouping-sets branch builds operators itself and hands them over with `builder.fromOp`, so the runtime binder wraps them in a `Legacy` leaf
- The input under the sets is already bound and optimized on the IR (`lowered(bind(child))`); only the expansion is opaque
- The IR already has the nodes needed: `Union`, `Aggregate`, `Project`, and `CTERef` over a `SharedCTE` for a result read by several readers

## Design

- Lane: Express — internal refactor of the runtime planner with the same operators; the launch prompt sets the direction and authorizes merge
- The grouping-sets expansion moves onto the plan builder: the builder gives a shared reader of the input reduced to the columns keys and aggregates read, and a union; `groupByVaryingKeys` becomes generic over the builder. Both planners run the same code
- Legacy planner: the builder builds exactly today's operators (`SharedResult` + `UnionAllOp`)
- Runtime planner: the shared input is a small `SharedCTE` (`GroupingSetInput`) whose readers are `CTERef`s; each set is `Project(Aggregate(CTERef))` (or `Aggregate` over constant keys when the set can't be global), under a `Union`. Lowering yields the same operator tree and labels
- NULL semantics stay exact by construction: rolled-up keys and the grouping id are constants of each set's projection, so a filter pushed through the union substitutes them (`k is null` becomes `null is null` on rolled-up sets); global sets (grand total) are never filtered below, as for any global aggregation
- `builder.fromOp` goes away
- Tests: spec/basic grouping-sets specs and EngineGroupingSetsTest on both planners, plus a unit test that the runtime plan has no `Legacy` node and a HAVING over rollup keys keeps its rows

## Alternatives and Why Not?

- A dedicated `GroupingSets` IR node lowering through the planner: every pass needs a new case and its own pushdown rule, while union/aggregate/project rules already hold
- Reusing `DeferredCTE` for the input: its deferred pruning and reader-filter pushing would change when and how the input is computed; the input is already pruned to the columns read
