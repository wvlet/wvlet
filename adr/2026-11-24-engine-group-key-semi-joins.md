# ADR: An aggregation joined by a group key aggregates only the keys its join can match

Date: 2026-11-24 · PR #2364

## Context

- A decorrelated scalar subquery becomes an aggregation by the correlation key, left-joined to the outer query (`Planner.decorrelateScalar`). TPC-H q17 at SF10 aggregates all 60M `lineitem` rows into 2M groups, but the join only looks up the ~2K keys of the filtered `part`; the aggregation was ~30% of the engine's CPU.
- DuckDB pushes the outer keys into such a subquery (a delim join).
- The runtime join filter is a row-group [min, max] range, which can't express a scattered set of keys.

## Decision

- A logical pass, `GroupKeySemiJoins` (`group_key_semi_joins`, on by default), runs after `ReorderJoins`. For a hash join that drops an aggregation's unmatched rows (an inner, left, or semi join with the aggregation on the right), it semi-joins the aggregation's input with a small relation on the other side that holds every non-NULL value of the join key, on the group key. A filter on a group key commutes with grouping.
- The key source is traced through filters, pass-through projections, and the key equalities of inner and semi joins (and a left join's right side). It must be a scan under filters and projections, or a shared CTE read, since it is read again; its estimated rows must be at most 1/8 of the group key's distinct values, and its scan no larger than the aggregation's input.
- The copy keeps its column ids: a semi join outputs only its left columns, so no node outputs both copies.
- It is a separate pass instead of a change to `decorrelateScalar`, so it also covers joins to grouped subqueries and CTEs, and leaves the Lean-modeled decorrelation as it is.

## Consequences

- TPC-H q17 at SF10 0.43–0.50x (2.8x → about 2x DuckDB), TPC-H SF10 suite 0.92x; TPC-DS q32 / q64 / q92 0.88–0.98x; other plans unchanged.
- Anti joins are left out (a NOT IN reads the subquery's NULL keys), as are right and full joins, which keep unmatched groups.
- The threshold depends on filter selectivity estimates: TPC-H q20's `p_name like 'forest%'` gets the default selectivity, so it is not reduced.

## Amendment (2026-11-24)

- The pass also matches a right join with the aggregation on the left that builds on the right, the mirror of a left join. The aggregation's side is lowered first there, but no runtime join filter from above reaches through such a join into its build side, so the copy of the key source takes none of the original's filters. A right join building on the left stays out.
- `prefix_like_selectivity` estimates `x like 'p%'` as a range with two bounds (0.09), so q20's `forest%` parts pass the 1/8 bar. q20 at SF10: 0.64x. See [the design note](../plans/2026-11-24-engine-prefix-like-selectivity.md).
