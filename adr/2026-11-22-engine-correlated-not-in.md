# ADR: Correlated NOT IN joins on the correlation keys with a null-aware residual

Date: 2026-11-22 · PR #2281

## Context

The engine decorrelates a filter conjunct `x NOT IN (select y from t where t.k = outer.k ...)` into an anti join. Until #2281 it was a hash anti join on `(x, k)`. NULL keys never match in `HashJoinOp`, so a row passed whenever its target was NULL or its correlation group held a NULL and no equal value, even when the group was not empty. SQL returns NULL for those rows, which a filter drops; DuckDB agrees. The same PR moved correlated `[NOT] EXISTS` / `[NOT] IN` onto the runtime plan, where the result had to be exact.

## Decision

- Correlated NOT IN is the NOT EXISTS rewrite: an anti join on the correlation keys only, with the residual `rest and (x = y or x is null or y is null)`. A row passes when no row of its group makes `x <> y` other than TRUE. An empty group passes, including for a NULL target or a NULL correlation key.
- The residual is built from bound expressions (the target bound in the outer scope, the value as a column of the body), not from a rewritten query expression. The residual binder resolves inner names first, so `a not in (select a from t2 where ...)` would otherwise compare the inner `a` with itself.
- The decorrelation is one function, `Planner.correlatedJoin[P]`, generic over the plan builder. The legacy planner calls it with operators and the runtime binder with runtime-plan nodes, so both planners build the same keys, residuals, and build sides, and the fix applies to both. (Amended 2026-11-23: M4b deleted the legacy planner; the runtime binder is the only caller.)
- Uncorrelated NOT IN is unchanged: its values are materialized at plan time, a NULL value gives no rows, and `x is not null` filters after a hash anti join on `x`.

## Consequences

- Correlated NOT IN loses `x` as a hash key, so a large correlation group is checked row by row in the residual. No TPC query uses correlated NOT IN. A null-aware anti join (tracking NULLs per group) could restore the key and is recorded as a follow-up.
- `decorrelate_correlated` off sends correlated conjuncts back through the planner. That reproduces main's golden plans byte for byte, but correlated NOT IN stays exact, since the planner shares the function. (Amended 2026-11-23: M4b removed the `decorrelate_correlated` toggle, whose only off state was this fallback; correlated conjuncts always bind on the runtime plan.)
