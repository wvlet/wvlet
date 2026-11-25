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
- Match `T ⋉ T on l.k = T.k, residual l.x <> T.x` where both sides are whole scans of the same table reading the same columns. Exclude float/double x (NaN ordering vs `<>`).
- Duplicate-insensitive form (q95): when the parent reads only k, the whole join becomes the group-by keys alone, as DuckDB does: one scan, one aggregation, no join.
- Otherwise: `filter(T, x is not null) ⋉ (T where k is not null group by k having min(x) <> max(x)) on k`. It stays a semi join, so bag semantics are kept.
- Not applied to a filtered left side (q94, q16, TPC-H q21), though the model covers it: the SF10 A/B showed q16 1.46x and q21 1.11x slower, since aggregating all of T costs more than the hash table it replaces when the probe side is small.
- New columns (min, max, group key) get ids from the query's binder through the optimizer context, keeping ids unique per query.
- Lean model next to `WvletFormal/SemiReduce/` (bag semantics with NULL x and NULL k), trace fixture, and a conformance test replaying it against the rule, per design/formal-verification.md.
- Tests: the conformance fixture (EXISTS, IN, DISTINCT over whole and filtered left sides, NULL x and k) on and off and against DuckDB; SF10 A/B on q95 plus unchanged-plan controls.

## Alternatives and Why Not?

- **Separate pass after ReduceSemiJoins.** Loses the duplicate-insensitive context and `needed` columns, and the CTE body path calls only `reduceBody`.
- **`count(distinct x) > 1` instead of min/max.** One call, but distinct counting is costlier than two running min/max values.
- **Also rewrite a filtered left side.** Covered by the model, but slower at SF10 (see above).
