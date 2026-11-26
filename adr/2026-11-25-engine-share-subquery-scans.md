# ADR: Share a plan-time subquery's filtered scan with the main query through a CTE bound in place

Date: 2026-11-25 · PR #2464

## Context

- An uncorrelated scalar subquery runs at plan time. When it filters a table that the main query also filters by the same conjuncts, the engine scanned the table twice. TPC-DS q44 reads `store_sales` of one store three times, and TPC-H q15 reads lineitem's quarter twice.
- #2447 and #2454 share an aggregation's groups when the subquery reads the aggregation's own input. Extending them to `count`/`avg` totals or NOT IN/EXISTS has no TPC-H or TPC-DS query to gain from at 63baa2f3. The repeated filtered scan does have such queries.

## Decision

- `share_subquery_scans` reads the table once as a shared CTE: the table filtered by the common conjuncts, keeping the columns the query names. It applies when a filter in a plan-time subquery and another filter of the query read the same catalog table. Each read keeps its other conjuncts as a filter over the CTE. The shared CTE machinery (`share_ctes`) materializes it once: the subquery computes it at plan time, and the main query scans its rows.
- The tree is not rewritten. Wvlet's tree transforms copy subquery relations (`Expression.transformPlan` always copies), and the planner keys subquery results by identity. Instead, each filter is registered by identity (`Planner.sharedScanOf`), and `RuntimeBinder.filter` binds its replacement where it stands. The CTE's reads are added to its reference count, and `readsCTE` counts registered filters as CTE reads.
- Conjuncts match by `VExpr.sig`, as other passes match conditions, because bound scalar functions hold closures.
- The CTE drops unnamed columns, so the rewrite applies only when no column can reach the output unnamed. It requires an explicit selection, and rules out `*` (except `count(*)`), set operations, natural joins, pivots, `dedup`, `describe`, and a `group by` without a selection.
- Two size guards apply:
  - The common conjuncts must be estimated to keep at most `CardinalityEstimator.DefaultSelectivity` of the table.
  - A HAVING subquery over its aggregation's exact input is left to `having_totals_from_groups`.

## Consequences

- At SF10, DS q44 runs at 0.56x and H q15 at 0.59x. H q22 stays neutral: the shared CTE of customers by country code costs about as much as the second scan.
- A shared read loses the source's scan-level pruning (Parquet row filters, runtime join filters on the scan) for its remaining conjuncts. The selectivity guard keeps the CTE small.
- Any future pass that matches `Filter` nodes structurally (like `planCorrelated`) sees the original filter. It reads the table directly, which is correct but unshared, so filters inside correlated subqueries are excluded from groups.
