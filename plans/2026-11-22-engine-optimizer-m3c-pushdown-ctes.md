# Engine optimizer M3c, PR 6: CTEs read once bind inline on the runtime plan

## Goals

- Let `PushDownFilters` (and the other logical passes) see into the body of a CTE read once, instead of stopping at a `Legacy` leaf.
- Keep the golden plans unless a filter now moves into a CTE body, with benchmark evidence for each change.

## Background

- A CTE read more than once is a `SharedCTE` read through `CTERef`s (#2273). A CTE read once (or every CTE, with `share_ctes` off) is inlined, but on the runtime planner the binder handed the reference to the legacy path (`planTable` → `planAliased`), so the body became a separately optimized `Legacy` leaf and a filter over it stayed above it.
- The legacy path already lowers an inlined CTE to a projection renaming the body's columns under the CTE's name (`planAliased`), the same thing the binder's `aliased` builds on the IR.
- Readers of a shared CTE come from the binder, the legacy planner, and nested runtime plans that are lowered before the outer plan is optimized, so the readers can't all agree on a filter yet.

## Design

Lane: Express. Internal planner change with no user-facing behavior change. The direction and gates came with the launch prompt.

- **Inline binding.** The binder binds the CTE's definition in place (`aliased(d)`), the same projection the legacy path builds. Filters then push through that projection into the body: below its aggregations, into its join trees, and into CTEs nested in it.
- **Shared CTEs unchanged.** Pushing into a `SharedCTE` only when every reader agrees is split into a follow-up. While some readers are legacy-planned or already lowered, the pass can't see every reader's conjuncts, so that half waits until readers are on the IR (Decorrelate / M4), or needs a deferred per-reader filter collected like the column union of #2273.
- **Golden plans.** 3 of 121 changed (TPC-DS q47, q57, q78). Their legacy plans equal main's, so they move to `spec/engine-plans/legacy/`.
  - q78: `where ss_sold_year = 2000` over the `ss` CTE now runs below its aggregation, so the aggregation reads fewer rows.
  - q47, q57: `v2` (read once) now binds inline, so the `d_year = 1999` filter and the `avg_monthly_sales` conjuncts reach the first `v1` reader inside the join tree. ReorderJoins then builds on the filtered side.
- **Benchmarks** (SF1, runtime vs `WVLET_ENGINE_PLANNER=legacy`, the same plans as main for these queries; 3 alternating rounds of 9 runs, sum of medians): q78 1048 → 851 ms (−19%), q47 197 → 200 ms, q57 95 → 91 ms. One full run: TPC-H total 1460 → 1459 ms, TPC-DS total 5316 → 5082 ms.
- **Tests.** `Optimizer.only(PushDownFilters)` cases: a filter reaches below the aggregation of a CTE read once, a nested CTE chain binds without `Legacy` leaves, and with `share_ctes` off every reader is inlined and filtered. Conformance on both planners, golden plans, and the toggle test cover the rest.

## Alternatives and Why Not?

- **Push into shared CTEs in this PR.** It needs every reader's conjuncts, which legacy-planned readers can't report. It is a separate, larger change.
- **Keep CTEs read once on the legacy path and push from the outside.** The body is already lowered by the time the outer plan is optimized, so there's no IR node to push into.
