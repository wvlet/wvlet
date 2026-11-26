# ADR: Read a top-N's pass-through strings back through a join on the declared key

Date: 2026-11-26 · PR #2499

## Context

TPC-H q10 keeps 20 of ~381K customers by revenue, but its customer scan decoded `c_name`, `c_address`, `c_phone` and `c_comment` for every one of them, and the joins carried those strings to the sort. Without those columns, q10 ran in 109 ms instead of 202 ms at SF10. DuckDB applies late materialization to top-N over a single table scan, fetching the remaining columns by row id. The engine's scans have no row ids that survive joins.

## Decision

- `late_columns` (`runtime/LateColumns.scala`) computes the top-N without a scan's pass-through string columns and joins them back from a second scan of the table on a declared unique key. It doesn't add row ids to scans and operators.
- It is a logical rewrite, so the second scan reuses existing machinery. The N keys become a per-row runtime filter and a row-group filter (`key_set_row_groups`), PruneColumns drops the strings below the top-N, and Lowering needs no change.
- Soundness rests on the key. Each key column must reach the sort and be read by an inner join on the way (declared keys may hold NULLs, and a NULL would not match in the join back). Uniqueness means each of the N rows matches exactly one row of the second scan. A sort over the N joined rows restores their order.
- Only string columns move, since carrying fixed-width columns costs little. The rewrite fires when the rows estimated at the sort are ≥ 64·N and ≥ 1/16 of the table's rows, so re-scanning the key column costs less than the strings it saves.

## Consequences

- q10 runs 0.94x wall and 0.92–0.93x CPU at SF10, less than the experiment's upper bound. The second scan still decompresses the string pages of most of customer's 13 row groups (SF10 files have one page per row group). The remaining gap is Snappy speed in uni.
- No golden plan changes: the golden scales never reach 64·N rows at the sort. `EngineLateColumnsTest` covers the rewrite.
- Tables without declared keys, columns under an aggregation, and LEFT or semi join paths are not rewritten. Row-id late materialization would cover them, but at the cost of threading ids through every operator.
