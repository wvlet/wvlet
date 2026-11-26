# ADR: Compute total expressions over a filter's dropped rows instead of gathering the kept ones

Date: 2026-11-25 · PR #2452

## Context

A filter hands its output on as deferred columns: source vectors plus the kept positions (`Batch.take`). The first reader gathers each column it reads into a new array. When a filter keeps most rows, the gather costs more than the arithmetic it feeds. TPC-H q1 keeps 98% of lineitem, and gathering six columns in the Project above the filter was about 14% of its CPU. Computing over every source row avoids the gather, but it evaluates expressions on rows the query excluded, so it must not change results or raise errors those rows would never have raised.

## Decision

- Behind `dense_selected_rows`, only *total* expressions are computed over the source rows: column references, constants, and `+` / `-` / `*` with a decimal or double result whose operands can't fail to convert. Double operands of decimal arithmetic are excluded, because NaN and infinities have no BigDecimal value. Int64 arithmetic (which throws on overflow), division, casts and functions are computed over the kept rows as before.
- The expression must read columns deferred at the *same* positions, and at least half the source rows must be kept. The result is then deferred at those positions, so a later reader sees exactly the kept rows. Column references alone pass through deferred at any selectivity, since that copies nothing.
- Aggregations read their arguments and string key codes at the positions (fused accumulators, `updateSelected`, `GroupTable.groupIdsAt`) instead of gathering them. Positions of -1 (outer-join NULL rows) fall back to the gathered path in the fused loop and the key lookup.

## Consequences

- A new expression kind becomes eligible only by adding it to `DenseSelectedRows.total`, and only if it can't throw on any input value.
- Filters that drop up to half of the rows spend up to 2x the arithmetic to skip gathers. That pays off for decimal arithmetic. It would not pay off for expensive functions, which is another reason they are excluded.
- Join outputs (separate probe and build positions) only gain the pass-through; expressions mixing both sides are computed as before.
