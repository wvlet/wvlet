# ADR: Reduce inner joins to semi joins where only the set of rows matters

Date: 2026-11-24

## Context

TPC-DS q95 at SF10 took 2.2–3.3 s against DuckDB's 54–75 ms. 80% of its CPU went to building two join results that no output row needs: the shared CTE `ws_wh` (`web_sales ⋈ web_sales` on `ws_order_number` with different warehouses, 74.8M rows) and `web_returns ⋈ ws_wh` (97.9M rows). Both are read only by `ws_order_number IN (...)`, which asks whether a value occurs. The join order was not the problem: both plans paid for the expansion.

## Decision

- A logical pass, `ReduceSemiJoins` (toggle `reduce_semi_joins`), runs after `PushDownFilters` and before `ReorderJoins`. In a duplicate-insensitive context, an inner join whose needed columns all come from some of its inputs (S) becomes a semi join of S with each connected group of the other inputs, on the same conditions: equalities across the two become keys, the rest the residual (`ws1.wh != ws2.wh`).
- Duplicate-insensitive contexts start at the subquery side of a semi or anti join, a DISTINCT, a group-by without aggregate calls, and INTERSECT / EXCEPT (not ALL). They pass through deterministic filters and projections, sorts, inner joins, the left side of semi and anti joins, and UNION ALL. Window, Limit, Sample, aggregate calls, and outer joins end them.
- A split needs an equality key per group, and S must stay connected by equalities, so that no rewrite adds a cross product (q38 and q87 took 400x longer before this check).
- A split semi join builds on the smaller side by estimated rows, as the binder does for IN; building on a large group made q14 7–14% slower.
- A shared CTE body is reduced when it is finalized, after its kept columns are known: the readers in the final operator tree (`finalizeCTEs`) report whether they are in such a context, and the body is reduced only when every one is and none reads every column. Readers in discarded plans (binder estimates) don't count; a plan-time read finalizes the body unreduced. A non-set reader created after reduction fails with UNEXPECTED_STATE, as for filtered bodies.
- The Lean model `formal/WvletFormal/SemiReduce/` proves that the projected inner join and the semi join have the same rows as sets, NULL conditions included, and that IN, EXISTS, NOT EXISTS, and NOT IN keep the same rows over either. `SemiReduceConformanceTest` replays its traces.

## Consequences

- q95 at SF10: 2,752 → 375 ms in the one-JVM A/B (0.14x); TPC-DS SF10 total 0.87x, TPC-H unchanged. SF1 suites unchanged (1.00x, 1.01x).
- 9 TPC-DS golden plans changed (q10, q14, q35, q37, q54, q69, q82, q95, q97).
- The reduced q95 still semi-joins all 7.2M `web_sales` rows against `ws_wh` before the dimension filters, and the CTE's semi join generates every warehouse pair before its residual. Those are separate follow-ups.
- Outer joins in a set context could also be reduced; they are left out until a query needs it.
