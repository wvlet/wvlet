# ADR: A correlated scalar subquery over an empty group gets its value over no rows

Date: 2026-11-22 · PR #2282

## Context

The engine decorrelates a correlated scalar subquery `(select agg(...) from t where t.k = outer.k)` into an aggregation of `t` grouped by `k`, left-joined to the outer input. An outer row whose group has no rows gets NULL from the left join. That is correct for aggregates whose value over no rows is NULL (avg, sum, min, max), but not for `count`, whose value over no rows is 0: this is the classic COUNT bug. `where (select count(*) from supplier where s_nationkey = n_nationkey and s_acctbal > 9900) = 0` returned no rows, where DuckDB returns 24. The same PR moved this decorrelation onto the runtime plan, where the result had to be exact.

## Decision

- At plan time, the select item is evaluated over a global aggregation of no rows (the grouping keys as NULL). When the result is NULL, the plan is the plain left join, unchanged.
- Otherwise, the joined value is projected as `case when <joined key> is null then <empty value> else value end`. The first right-side key decides whether a row matched: it is never NULL on a matched row, because hash joins don't match NULL keys and the join has no residual.
- The key check, not `coalesce(value, <empty value>)`, keeps a matched group's NULL value. For example, `case when count(*) > 0 then null else 1 end` is NULL for a non-empty group.
- The decorrelation (`bindWithSubqueries`, `decorrelateScalar`, `decorrelateExists`) is generic over the plan builder and shared by both planners, so the fix applies to both.

## Consequences

- Queries whose scalar subquery aggregates to non-NULL over no rows (`count`, or `coalesce(sum(x), 0)`) get one extra projection after the left join. In TPC-DS q41 it merges into the filter above it.
- `decorrelate_scalar` off sends these conjunct lists back through the planner. That reproduces main's golden plans except for queries the fix changes (TPC-DS q41).
