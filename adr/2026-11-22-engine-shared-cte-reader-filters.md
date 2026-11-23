# ADR: Shared CTE bodies keep only the rows every reader's filter agrees on

Date: 2026-11-22 · PR #2277

## Context

A shared CTE (read more than once) is computed once and read from memory; since #2273 its body is lowered at the end of the outermost `Planner.plan` (ADR 2026-11-22-engine-shared-cte-deferred-pruning). Many readers keep only a few of its rows (TPC-DS q04/q11/q74 read `year_total` six times, each for one or two years), but the body computed every row. Readers come from the binder, the legacy planner, and nested runtime plans lowered before the outer plan is optimized, so no logical plan holds every reader's filter.

## Decision

- Readers report their filter when they become operators: `Lowering` passes the condition of `Filter(CTERef)`, over the body's column ids, to `SharedCTE.reader`. A bare `CTERef`, a legacy-planned reader, or an iterator reader reports none. Every reader created counts, including ones the planner discards.
- When the body is finalized and every reader reported a filter, it gets the conjuncts every reader has and, per column, the OR of each reader's conditions on that column alone. Conditions over several columns or over aggregate outputs stay with their readers. Each reader keeps its own filter, so the body only needs to keep the rows some reader keeps.
- The conditions are pushed into the already optimized body with `PushDownFilters`, and may pass a filter directly over an aggregation (HAVING), not other filters, which may guard expressions the conditions would fail on.
- A read before finalization (plan-time subqueries) keeps every row. A reader created after the body was filtered fails with UNEXPECTED_STATE, since the body can't be recomputed.
- `push_down_cte_filters` turns it off; off, the plans equal those before the change.

## Consequences

- 7 TPC-DS golden plans changed (q04, q11, q31, q39, q64, q74, q75); the changed queries ran 17% faster at SF1.
- A body with a legacy-planned reader (q95's IN subquery) stays unfiltered until Decorrelate/M4 puts those readers on the IR. After M4 the filters can come from the `CTERef`s of the logical plan.
- A filter that reaches an ordered inner join inside the body stays above it, as for aggregation inputs today.
