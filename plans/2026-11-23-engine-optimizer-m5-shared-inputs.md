# M5: Re-plan grouping-sets inputs over computed shared CTEs

## Goals

- Joins inside a grouping-sets (or unpivot) input get the exact row counts of the shared CTEs they read, as joins elsewhere do since M5 PRs 1–3.
- Target: TPC-DS q14, whose `cross_items` IN semi joins under `rollup` build on the `catalog_sales` / `web_sales` trees because `cross_items` is estimated at 1e6.

## Background

- The binder plans the input of grouping sets once and shares it among the per-set aggregations as a `SharedInput` (a `SharedCTE` read through `CTERef`s). Its body and operator are fixed at bind time.
- The re-plan step (`Planner.replanRuntime`) walks the root's bound plan, but a `CTERef` is a leaf: it never looks into a `SharedInput` body, so CTEs read there are not computed, and the joins there are not re-decided.
- Exact counts (DuckDB): `cross_items` has 1,573 rows at SF0.1 and 16,193 at SF1. The catalog and web trees it is joined with have 4,502 / 2,365 rows at SF0.1 and 46,691 / 23,529 at SF1. With exact counts, the binder's build-side rule (`swapBuild`) keeps the build on `cross_items`, so the golden (SF0.1) q14 plan changes (`RightSemi` → `Semi`, twice). At SF1 the trees' estimates already exceed the 1e6 bound by enough, so SF1 plans are unchanged.

## Design

- Lane: Express. An internal optimizer change behind a toggle, with no user-facing surface; merge is pre-authorized by the launch prompt.
- The re-plan step follows a `SharedInput`'s body: shared CTEs read below joins there are computed, like those in the root plan.
- When a body reads a computed CTE, it is refreshed the way bound inputs are (PR 2): the inputs the binder optimized are optimized again, and semi/anti build sides are chosen again (PR 3). If the body changes, a new `SharedInput` is lowered from it, and every reader of the old one reads the new one. The output columns keep their order.
- A body that doesn't change keeps its `SharedInput`, so plans without such reads stay identical.
- Toggle `replan_shared_inputs`, under `replan_materialized_ctes`.
- Tests: a unit test in `EngineMaterializedCTEStatsTest` (rollup over an IN against a small shared CTE), plus golden q14 and the conformance suites.

## Not in this PR

- Plan-time IN/EXISTS subquery results (uncorrelated subqueries computed while planning) as exact-stats inputs. This stays on the M5 item.

## Alternatives and Why Not?

- Re-bind the whole grouping-sets aggregation: this repeats binding and its plan-time subquery evaluation. Refreshing the body reuses PR 2's mechanism.
