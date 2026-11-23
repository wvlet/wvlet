# Engine optimizer M3c, PR 3: PruneColumns by id, across shared CTE boundaries

## Goals

- Compute a shared CTE (a CTE read more than once) with only the columns its readers use, the union of their needs, instead of every column of its body (framework pain point 7).
- Add `PruneColumns` as a logical pass over the runtime plan, by column id, so that CTE bodies are pruned on the IR, and later passes (PushDownFilters into CTE bodies, M4) have a CTE node to work on.
- Keep golden plans unchanged outside shared CTEs; show the CTE plan changes with benchmark evidence.

## Background

- Today a shared CTE is planned when its first reader is planned: `Planner.planTable` lowers the body and runs the physical passes over it alone (`optimizePhysical`), keeping its whole output. Readers are scans of the materialized result (`SharedTable`), which `ColumnPruner` narrows by name, but nothing tells the body.
- Readers come from three places: the runtime binder (joins, filters, projections), the legacy planner (set operations, subqueries such as q95's `IN (select ... from ws_wh)`, windows), and nested runtime plans inside `Legacy` leaves, which are bound, optimized, and lowered before the outer plan is done. So the union of the readers' needs is only known once the whole query is planned and physically pruned.
- Scalar subqueries and uncorrelated EXISTS/IN subqueries run at plan time (q14 `avg_sales`), so a CTE result may be needed before planning ends. Subqueries are planned by the same `Planner` (`Planner.nested(p) = p`), re-entering `plan`.
- 20 of 121 golden plans read a shared CTE (TPC-DS q01, q02, q04, q11, q14, q23, q24, q27, q30, q31, q36, q39, q47, q57, q59, q64, q74, q75, q81, q95).

## Design

Lane: Express. Internal planner change with no user-facing behavior change; the direction (scope, gates) was given in the launch prompt.

- **IR nodes.** A `CTERef(cte, columns)` leaf reads a shared CTE; its columns are fresh ids matching the body's output by position. The CTE itself (`SharedCTE`: name, the bound body as a `RuntimePlan`, its result) is one object per CTE, shared by every reader. With the runtime planner, the binder binds a shared CTE reference to a `CTERef`, and the legacy planner's readers read the same `SharedCTE`, so the body is still computed once.
- **Deferred body.** The body is bound and optimized at the first reference, as today, but lowered only when the whole query is planned (at the end of the outermost `Planner.plan`, not the nested subquery plans): the needed columns are the union of the columns every reader scan reads after physical pruning. The body is then pruned by `PruneColumns` to them, lowered, and physically optimized. Reader scans resolve their columns against the finalized result, so they read the same rows by name.
- **Order.** A CTE body that reads another CTE is finalized first, so its readers are known before the CTE it reads.
- **Keep every column** when a reader can't tell what it reads: any read of a result before it is finalized (scalar, EXISTS, IN subqueries run at plan time), a reader over duplicate column names (an iterator, not a scan), or pruning turned off. A scan asking for a column the union missed recomputes the full result rather than fail, and fails in tests (`validatePlans`).
- **`PruneColumns` pass** (last logical pass): top-down needs by id, matching what `ColumnPruner` does today so that plans without shared CTEs don't change. It drops unused projection expressions; filters, sorts, limits, aggregates (which keep their calls), and joins pass needs down with the columns they read; scans, `Values`, `CTERef`, and `Legacy` leaves keep their columns (the physical pass narrows scans together with their row-group filters until PushScanFilters moves). The root keeps its output. `ColumnPruner` still runs after lowering.
- **One toggle.** `prune_columns` turns off both the logical and the physical pruning, as the logical pass replaces the physical one over time; the toggle test then covers both.
- **Unchanged.** The legacy planner keeps today's eager `SharedResult`, so its golden plans of the 20 queries move to `spec/engine-plans/legacy/` where they differ. CTEs read once stay inlined through the legacy path. `share_ctes` off inlines as today; `prune_columns` off keeps every body column.
- **Tests.** `Optimizer.only(PruneColumns)` cases per node; a CTE read by several readers (binder and legacy, nested CTEs, a plan-time scalar read) computes only the union; golden plans, conformance on both planners, and the toggle test for the rest.

## Alternatives and Why Not?

- **Prune by id alone, from the `CTERef`s of the logical plan.** Readers inside `Legacy` leaves and nested runtime plans are already operators, so their needs are invisible; it would have to keep every column for most CTEs.
- **Prune physically only (extend `ColumnPruner` to shared results).** Works the same today, but leaves the CTE outside the IR, so PushDownFilters into CTE bodies would have no node to push into.
- **Bind CTEs read once inline on the IR.** Lets pushdown and join order see into them, but changes many plans; it belongs with PushDownFilters into CTE bodies.
