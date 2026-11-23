# ADR: Joins over shared CTEs are ordered again with the CTEs' exact row counts

Date: 2026-11-23 · PRs (M5, PR 1 wvlet/wvlet#2300, PR 2)

## Context

A shared CTE (read more than once) is computed once and read from memory. Its body is lowered at the end of the outermost `Planner.plan`, with the union of its readers' columns and the rows their filters agree on (ADRs 2026-11-22-engine-shared-cte-deferred-pruning and -reader-filters). Until it is computed, its row count is unknown: `Stats` and `CardinalityEstimator` both use 1e6, so join order and build sides treat every shared CTE as a large table. The framework plan (section 4, decision D7) makes adaptive re-planning part of M5: a result computed by the engine gets exact statistics, and `ReorderJoins` runs again over the joins that read it.

## Decision

- After the outermost plan finalizes its shared CTEs, the planner looks in the root's bound runtime plan for the joins `ReorderJoins` still decides: `InnerJoins` graphs, and outer hash joins, whose build side it picks. If a `CTERef` sits below one of them, the planner computes those CTEs and records their exact row counts. `Stats.rows(CTERef)` and the CTE table's `estimatedRows` (read by `CardinalityEstimator`) both return the count.
- The logical passes run again over the root's bound plan, with an optimizer of their own so that `firedRules` counts each rule once. When the result differs from the first optimized plan, it is lowered again and replaces the first operator.
- The CTE is computed before its readers are planned a second time, so they can't change what the result keeps. A new reader is allowed after the body was filtered when its filter equals one a reader already reported, since that filter implies the pushed conditions. If a new reader's scan reads a column the result dropped, or a reader is refused, the planner keeps the first operator; with `validatePlans` (tests) it raises the error instead.
- Inputs the binder optimizes while binding (aggregation, pivot, and subquery inputs) already hold ordered joins in the bound plan. The binder records each one's bound plan; the step follows those to find CTEs below their joins, and optimizes such an input again from its bound plan before the root passes run (PR 2, toggle `replan_bound_cte_joins`). A re-ordered input outputs its columns in its new join order, so it is wrapped in a projection that restores the first order, which a parent such as a union reads by position.
- `replan_materialized_ctes` turns it off. With the toggle off, plans are identical to those before the change and CTEs run at execution, as before.

## Consequences

- `explain` computes the shared CTEs that such joins read, as it already computes uncorrelated scalar subqueries. A CTE that execution would read only in part (under a LIMIT, or behind an empty build side) is computed in full, and an error in its body is raised while planning.
- 3 TPC-DS golden plans changed (q02, q31, q59), where a small CTE result (one row per week, or per county and quarter) becomes the build side. TPC-H plans are unchanged.
- Some joins don't get the exact counts yet: build sides the binder picks for joins it builds itself (correlated and semi joins), joins inside CTE bodies that read other shared CTEs, and per-reader statistics such as rows after the reader's filter or the distinct counts of join keys. These are M5 follow-ups.
