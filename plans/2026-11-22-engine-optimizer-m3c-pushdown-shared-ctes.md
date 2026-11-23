# Engine optimizer M3c, PR 7: PushDownFilters into shared CTE bodies

## Goals

- Let a filter that every reader of a shared CTE (a CTE read more than once) applies reach the CTE body, so the body computes fewer rows (TPC-DS q04/q11/q74 read `year_total` six times, each for one or two years).
- Keep golden plans unless a filter now moves into a shared CTE body, with benchmark evidence for each change.

## Background

- A shared CTE is a `SharedCTE` read through `CTERef`s; its body is bound and optimized at the first read and lowered at the end of the outermost `Planner.plan`, keeping the union of the columns its readers read (#2273, ADR 2026-11-22-engine-shared-cte-deferred-pruning).
- Readers come from the binder (`CTERef` on the IR, lowered to a scan of the result), the legacy planner (a scan built directly), and nested runtime plans lowered before the outer plan is optimized. The logical plan never holds every reader, but every reader becomes an operator through one of two calls on the `SharedCTE`.
- A reader's own filter stays in place, so pushing into the body is valid for any predicate implied by every reader's filter: the body may compute more rows than a reader needs, never fewer.

## Design

Lane: Express. Internal planner change with no user-facing behavior change; direction and gates came with the launch prompt.

- **Readers report at lowering.** When `Lowering` turns `Filter(CTERef)` into operators, it hands the reader's conjuncts (over the body's column ids) to the CTE along with the reader. A bare `CTERef`, a legacy-planned reader, or an iterator reader reports none. Every reader created counts, including ones planned and discarded (e.g. while probing a subquery for correlation): extra readers only make the body unfiltered.
- **Agreement at finalization.** The filter is decided once, when the body is finalized at the end of the outermost plan. When every reader reported conjuncts, the body gets:
  - the conjuncts every reader has, and
  - per body column, the OR of each reader's conjuncts over that column alone, when every reader has one (q04: `dyear = 2002 or dyear = 2001`). Conjuncts over several columns, or on columns not every reader restricts, stay with their readers only.
- **Unfiltered otherwise:** any reader without conjuncts, a read before finalization (plan-time subqueries), or `push_down_filters` off.
- **Pushing into the body.** The predicate is pushed with `PushDownFilters` over the optimized body before column pruning, so it moves through projections, union branches, and aggregation keys as any filter does. Join order is already decided; a filter reaching an ordered inner join stays above it, as for aggregation inputs today (notes from #2271).
- **Through HAVING filters.** The pass normally keeps conditions from above a filter above it; over an already optimized body that would leave the predicate above a HAVING filter, after the aggregation (q39 ran ~20% slower that way). This entry point moves them below a filter directly over an aggregation, together with its own conjuncts; other filters (which may guard a cast the conditions would fail on) still stop them.
- **Constant conjuncts.** A conjunct that becomes constant true after substitution (a union branch projecting `'s' as sale_type`) is dropped instead of kept as a filter.
- **Late readers fail loudly.** A reader created after the body was finalized with a filter raises UNEXPECTED_STATE, as a late read of a dropped column does today.
- **Tests.** Planner cases: a shared CTE read with different years gets the OR in its body; one unfiltered reader, or a legacy-planned reader, keeps the body unfiltered; results match `share_ctes` off. Conformance on both planners, golden plans, and the toggle test cover the rest.

## Alternatives and Why Not?

- **Collect conjuncts from the logical plan after optimization.** Readers inside `Legacy` leaves and nested plans are invisible there, and a `CTERef` could be lowered from a plan other than the one that reported; reporting at lowering sees exactly the readers that exist.
- **Push the full OR of the readers' filters.** It refers to aggregate outputs (`year_total > 0`) and can't pass the body's aggregation; the per-column OR can.
- **Wait for Decorrelate / M4.** Most TPC shared CTE readers are already on the IR, so waiting leaves the gain on the table.
