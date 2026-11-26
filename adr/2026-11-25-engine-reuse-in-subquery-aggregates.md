# ADR: Replace a joined table by an IN subquery's groups instead of sharing groups computed at plan time

Date: 2026-11-25 · PR #2468

## Context

- TPC-H q18 sums `l_quantity` per order over `customer ⋈ orders ⋈ lineitem`, filtered by `o_orderkey in (from lineitem group by l_orderkey where l_quantity.sum > 300 select l_orderkey)`. The engine read lineitem twice: the uncorrelated IN subquery ran at plan time (about 60% of q18's wall time after #2460), then the join read lineitem again under a join filter.
- `share_in_subquery_groups` (#2454) and `having_totals_from_groups` (#2447) share groups computed at plan time, but only when the subquery aggregates the aggregation's own input. q18's subquery aggregates one table of the join, by the join key.

## Decision

- `reuse_in_subquery_aggregates` rewrites the aggregation's input before planning it: the joined table is replaced by the subquery's own grouping, with each aggregated column's per-key total named as the column, and the IN conjunct is dropped because the inner join on the key implies it. This is DuckDB's aggregate reuse, written as eager aggregation with the subquery's key filter attached.
- It applies when all of these hold:
  - The subquery is `from t group by k [where ...] select k` over a catalog table.
  - The aggregation's input is an inner-join tree of catalog tables that reads `t` once.
  - A top-level conjunct (in WHERE or ON) equates `k` with a column of another table. This conjunct rejects the NULL key's group, as the dropped IN did.
  - The IN target is `k` or that column.
  - Every aggregate call is a `sum` (over integers or decimals), `min` or `max` of one column of `t`.
  - No other column of `t` is read outside those calls.
  - The names of the columns of `t` that the query reads appear in no other table.
- It is correct for any multiplicity of the other tables' rows. Each of their rows met every row of `t` with its key, and now meets that key's total once. A sum of sums, a min of mins and a max of maxes give the same values. `count(*)`, `avg`, and calls over other tables' columns would see one row per key, so they disable the rewrite.
- The rewrite is a new logical input that `aggregation` plans again, falling back to the original input if that fails. It doesn't use groups computed at plan time: the subquery's aggregation runs once at run time, in parallel, and becomes the join's build side. No tree transform copies a subquery, because the dropped IN conjunct is the only subquery involved.

## Consequences

- At SF10, H q18 runs at 0.77x wall and 0.84x CPU. H q17, q20, q2, q11 and q15 are neutral. Only q18's golden plan changes.
- q18 is now bound by the lineitem aggregation over 15M keys (partial HashAgg plus PartitionedAgg). Merging morsel partials of sorted keys without repartitioning is a separate follow-up.
