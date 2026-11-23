# Engine optimizer M5, PR 3: re-decide the binder's semi-join build sides over shared CTEs

## Goals

- Semi and anti joins the binder builds itself (uncorrelated IN, EXISTS / NOT EXISTS, correlated IN) pick their build side when bound, while a shared CTE below them is still estimated at 1e6 rows. Once the re-plan (PRs 1–2, ADR 2026-11-23-engine-replan-materialized-ctes) knows the CTE's exact rows, re-decide those build sides.
- Keep it small: the build side only. Where the binder places an IN semi join (on one input vs. over the ordered tree) stays a follow-up.

## Background

- The binder decides `buildLeft` for a semi join as `swapBuild(rows(subquery), rows(outer))` (`plans.inJoin`, `Planner.correlatedJoin`); with a 1e6 CTE that builds on the outer side (`RightSemi`).
- TPC-DS q95 hits this: its two `ws_order_number in (...)` semi joins over the `ws_wh` CTE build on the `web_sales` join tree. q14's `cross_items` semi joins look alike but sit under a grouping-sets input (`rollup`), which the re-plan doesn't follow yet (a separate follow-up).
- PR 1's re-plan computes only CTEs below `InnerJoins` or outer hash joins, so a CTE read only by semi joins isn't computed today. `ReorderJoins` re-decides outer hash joins' build side, but not semi or anti joins.
- `Stats.rows` of a semi or anti join is its outer input's rows on either build side, so flipping the build side changes no estimate above it.

## Design

- Lane: Express. An internal planner change behind a toggle, no user-facing behaviour or always-gate area; plan changes are checked with golden plans and benchmarks.
- The re-plan also counts CTEs read below semi and anti hash joins (with keys) as joined, so it computes them.
- In the re-plan's optimizer run only, `ReorderJoins` re-decides the build side of a semi or anti hash join when one of its inputs reads a computed CTE, by the binder's rule (`swapBuild(rows(right), rows(left))`). The first run is unchanged, so plans that read no shared CTE stay as they are.
- Semi and anti joins keep their results on either build side (the engine already builds both forms, `RightSemi` / `RightAnti`), and NOT IN's NULL handling sits outside the join, so the flip keeps NULL semantics.
- Toggle `replan_semi_join_builds` (on by default; only acts with `replan_materialized_ctes`) for A/B on one build.
- Tests: an IN over a 2-row shared CTE flips from `RightSemi` to `Semi`; toggle off keeps the earlier plan; rows match. Conformance over spec/basic, TPC-H, TPC-DS; q95's golden plan is expected to change.

## Alternatives and Why Not?

- Re-decide every semi/anti join in every run: Stats and CardinalityEstimator may differ on bound inputs, which could flip plans that read no CTE.
- Record the binder's decision and replay it with new estimates: more state for the same rule; the join node already carries what the rule needs.
- Also re-place IN semi joins (onto one input once the CTE proves small): changes the plan's shape, not just a flag; a follow-up.
