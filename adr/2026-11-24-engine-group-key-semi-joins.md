# ADR: An aggregation joined by a group key aggregates only the keys its join can match

Date: 2026-11-24 · PR #TBD

## Context

- A decorrelated scalar subquery becomes an aggregation by the correlation key, left-joined to the outer query (`Planner.decorrelateScalar`). TPC-H q17 at SF10 aggregates all 60M `lineitem` rows into 2M groups, but the join only looks up the ~2K keys of the filtered `part`; the aggregation was ~30% of the engine's CPU.
- DuckDB pushes the outer keys into such a subquery (a delim join).
- The runtime join filter is a row-group [min, max] range, which can't express a scattered set of keys.

## Decision

- A logical pass, `GroupKeySemiJoins` (`group_key_semi_joins`, on by default), runs after `ReorderJoins`. For a hash join that drops an aggregation's unmatched rows (inner, or the aggregation on the right of a left or semi join), it semi-joins the aggregation's input with a small relation on the other side that holds every non-NULL value of the join key, on the group key. A filter on a group key commutes with grouping.
- The key source is traced through filters, pass-through projections, and the key equalities of inner and semi joins (and a left join's right side). It must be a scan under filters and projections, or a shared CTE read, since it is read again; its estimated rows must be at most 1/8 of the group key's distinct values, and its scan no larger than the aggregation's input.
- The copy keeps its column ids: a semi join outputs only its left columns, so no node outputs both copies.
- It is a separate pass instead of a change to `decorrelateScalar`, so it also covers joins to grouped subqueries and CTEs, and leaves the Lean-modeled decorrelation as it is.

## Consequences

- TPC-H q17 at SF10 0.43–0.50x (2.8x → about 2x DuckDB), TPC-H SF10 suite 0.92x; TPC-DS q32 / q64 / q92 0.88–0.98x; other plans unchanged.
- Anti joins are left out (a NOT IN reads the subquery's NULL keys), as are right and full joins, which keep unmatched groups.
- The threshold depends on filter selectivity estimates: TPC-H q20's `p_name like 'forest%'` gets the default selectivity, so it is not reduced.
