# Engine optimizer M5, PR 4: re-plan shared CTE bodies that join other shared CTEs

## Goals

- A shared CTE body that joins another shared CTE (e.g. `u` joining `t` twice, with `u` read twice) is ordered when bound, with the inner CTE estimated at 1e6 rows. The re-plan (PRs 1–3, ADR 2026-11-23-engine-replan-materialized-ctes) orders only the root's joins, so re-plan such a body once the inner CTE is computed.
- Keep it small: bodies only. Placement of IN semi joins, grouping-sets inputs, and per-reader statistics stay follow-ups.

## Background

- `finalizeCTEs` lowers a body before the CTEs it reads: the inner CTE's columns and rows come from the outer body's readers. So the inner CTE can't be computed before the outer body is lowered.
- The first remaining sub-entry (re-place IN semi joins over a CTE) reaches no benchmark plan: only q95 has an IN over a shared CTE on one input. At SF1 `ws_wh` has 6.6M rows against 719K `web_sales` rows (and `web_returns ⋈ ws_wh` estimates as `ws_wh` once ordered), so the IN stays over the tree. At SF0.1 `ws_wh` is empty, so placement would only change the plan of an empty input.
- No TPC-H or TPC-DS query has a shared CTE body that reads another shared CTE either (bodies like q47's `v2` or q64's `cross_sales` are read once, or read their inner CTE only once, so they are inlined). The change targets model-style queries with chained CTEs; benchmarks don't reach it.

## Design

- Lane: Express. An internal planner change behind a toggle, no user-facing behaviour or always-gate area.
- A shared CTE keeps its bound body next to the optimized one. After `finalizeCTEs` and before the root's re-plan, the CTE bodies that read other shared CTEs are re-planned innermost first (reverse finalize order). Each one goes through the same step as the root: compute the CTEs its joins read, optimize its bound body again, and keep the result when it differs.
- The new body keeps the first result's columns (`keep`) and the reader filters pushed into it, with a `Project` to restore the body's column order. Its readers of the inner CTE must read kept columns and an already-reported filter, as in PR 1; otherwise the first body is kept (with `validatePlans`, the error is raised).
- A body that was already computed (read at plan time) is left as it is.
- Toggle `replan_cte_bodies` (on by default; only acts with `replan_materialized_ctes`).
- Tests: `u` joining a 2-row `t` twice probes `orders` once re-planned (CTE `t` without it); filters pushed into both bodies; rows match with the re-plan off. Golden plans are unchanged, as no benchmark query reaches the step; `EngineOptimizerToggleTest` skips its plan-change check for this toggle with that reason.

## Alternatives and Why Not?

- Finalize the inner CTE first: its columns and rows depend on the outer body's readers, so it would lose column pruning and reader-filter pushdown.
- Lower the outer body twice during `finalizeCTEs` (before and after the inner one is computed): the same work with more coupling to the finalize loop.
