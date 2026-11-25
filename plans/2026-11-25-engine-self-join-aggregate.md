# Rewrite self semi joins on a key with a different value into GROUP BY HAVING

## Goals

- Close most of the TPC-DS q95 gap at SF10 (206 ms vs DuckDB 57 ms on 034df78d) and help q94 (80 vs 27 ms), whose cost is a semi join of web_sales with itself on `ws_order_number` with residual `ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk`.

## Background

- q95's `ws_wh` CTE is read only through `ws_order_number` by IN subqueries, so `ReduceSemiJoins.reduceBody` already turns the self inner join into `web_sales ⋉ web_sales on k, residual x1 <> x2`. That semi join still builds a 7.2M-row hash table and checks the residual against every duplicate of a key, then probes all 7.2M rows.
- q94's `exists { from web_sales ws2 where ws1.k = ws2.k and ws1.x <> ws2.x }` decorrelates to the same semi join, with a filtered ws1 on the left.
- DuckDB 2.0.1 `SELF_JOIN_AGGREGATE` (written for q95) replaces the self join, read only through K in a duplicate-insensitive parent, by `SELECT K FROM T GROUP BY K HAVING min(x) <> max(x)`.
- Key fact: when every left row is itself a row of T, "some row of T with the same k has a different x" holds exactly when the left row's x is not NULL and its k group has two distinct non-NULL x values, i.e. `min(x) <> max(x)` (min/max skip NULLs; a NULL k never matches `=`).

## Design

Lane: Express — an engine optimization behind a new toggle, pre-approved by the engine optimization loop; no user-facing surface.

- New toggle `self_join_aggregate`, applied inside the `ReduceSemiJoins` walk (main plans and shared CTE bodies), so it sees the semi joins that pass creates and its duplicate-insensitive context.
- Match `L ⋉ R on L.k = R.k, residual L.x <> R.x` where R is a bare full scan of table T, and L's k and x trace (through filters, column projections, inner joins, the left side of semi/anti joins, sorts, limits) to one scan of the same T reading the same columns. Exclude float/double x (NaN ordering vs `<>`).
- General form (q94): `filter(L, x is not null) ⋉ (T group by k having min(x) <> max(x)) on k`. It stays a semi join over L, so bag semantics are kept without needing a duplicate-insensitive context.
- Duplicate-insensitive form (q95): when L is also a bare full scan of T and the parent reads only k, the whole join becomes the group-by keys alone, as DuckDB does: one scan, one aggregation, no join.
- New columns (min, max, group key) get ids from the query's binder through the optimizer context, keeping ids unique per query.
- Lean model next to `WvletFormal/SemiReduce/` (bag semantics with NULL x and NULL k), trace fixture, and a conformance test replaying it against the rule, per design/formal-verification.md.
- Tests: rule fires / doesn't fire (filtered R, different table, different column, float x, needed x), result equality with the toggle off including NULL x/k; golden plans for q95/q94 regenerated; SF10 A/B on q95, q94 plus unchanged-plan controls.

## Alternatives and Why Not?

- **Separate pass after ReduceSemiJoins.** Loses the duplicate-insensitive context and `needed` columns, and the CTE body path calls only `reduceBody`.
- **`count(distinct x) > 1` instead of min/max.** One call, but distinct counting is costlier than two running min/max values.
- **Only the duplicate-insensitive form.** Misses q94, whose left side is a filtered join tree.
