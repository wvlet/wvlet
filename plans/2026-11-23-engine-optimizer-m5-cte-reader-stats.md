# Engine optimizer M5, PR 5: estimate computed shared CTE reads from their results

## Goals

- Once the re-plan (PRs 1–4, ADR 2026-11-23-engine-replan-materialized-ctes) computes a shared CTE, `ReorderJoins` knows its total rows but still guesses everything else about each read: the rows a reader's own filter keeps (0.3 per condition, or 1/ndv with an unknown ndv) and the distinct values of its join keys (unknown, so the smaller side is assumed to hold a unique key).
- Read both from the computed result instead, so joins over a CTE read with different filters (e.g. TPC-DS `year_total` read six times by sale type and year) are ordered by what each read actually keeps.

## Background

- `Stats.distinct` and `CardinalityEstimator.columnDistinct` return None for a CTE read, so `x = literal` filters over it fall back to the default selectivity and join outputs use the primary-key assumption.
- A reader's filter stays a single-input condition of the `InnerJoins` graph in the bound plan; `ReorderJoins.order` estimates the input as rows × selectivities before pushing it.
- A probe that only adds exact distinct counts changes TPC-DS q04, q11, q31 and q74; with exact filtered rows too, q04, q11, q31, q47, q57 and q74 change. TPC-H plans are unchanged (no TPC-H query has a shared CTE under a join).

## Design

- Lane: Express. An internal planner change behind a toggle; no user-facing behaviour or always-gate area.
- Only CTEs the re-plan already computed are read; nothing is computed for statistics alone. Results over 2^20 rows keep the estimates, bounding the extra pass (about 15 ms over q95's 6.6M-row `ws_wh` at SF1, for no plan change).
- Distinct counts: the number of distinct non-NULL values of a result column, computed on first use and cached per column. `Stats.distinct(CTERef)` and the CTE table's `estimatedDistinct` (read by `CardinalityEstimator`) both return it, so the two estimators stay in step.
- Filtered rows: when an `InnerJoins` input is a computed CTE read with single-input conditions, its estimate is the number of result rows for which their conjunction is TRUE (NULL and FALSE drop the row, as the filter does). Cached per condition. If a condition reads a column the result dropped, or fails to evaluate, the usual estimate is used.
- Toggle `cte_reader_stats` (on by default; acts only on CTEs computed by `replan_materialized_ctes`). With it off, plans are as before.
- Tests: a query over a CTE read twice with different filters where the exact counts pick another order; rows match with the toggle off. Golden plans regenerate with benchmark evidence (SF0.1 and SF1, A/A noise round).

## Alternatives and Why Not?

- Keep histograms or samples of the result: exact counts are cheap here, since the result is already in memory.
- Estimate filtered rows outside `InnerJoins` (outer join inputs, semi join sides): `Stats.rows` ignores filters everywhere else, as `CardinalityEstimator` does; changing that is a wider estimator change, left as a follow-up.
