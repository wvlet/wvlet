# ADR: Compute a correlated min, max, or avg as a window over the outer join's rows

Date: 2026-11-26 · PR #TBD

## Context

TPC-H q17 compares each `lineitem ⋈ part` row with `0.2 * avg(l_quantity)` over the lineitem rows of its part, and q2 compares each `part ⋈ partsupp ⋈ supplier ⋈ nation ⋈ region` row with `min(ps_supplycost)` over the partsupp rows of its part in the region. The planner decorrelates the subquery into an aggregation per key joined back (`Planner.decorrelateScalar`), and `group_key_semi_joins` shrinks the aggregation to the outer keys. The table is still read twice. At SF10 both queries were Snappy-bound (60-65% of engine CPU in page decompression), so the second read of lineitem (q17) or partsupp (q2) was the largest cost left. DuckDB reads the table twice as well (a CTE of the outer join, then a second scan semi-joined to its keys).

## Decision

A logical pass, `aggregate_windows` (`runtime/AggregateWindows.scala`), runs right after `group_key_semi_joins`. It turns `O ⋈ (I group by g agg f(x))` on the key into `O` with `f(x) over (partition by g)`, the WinMagic rewrite, when:

- O is I joined with other relations E. Every leaf of I matches a leaf of O reading the same rows, every condition of I is a condition of O, and O's other conditions read only E or equate g with a column of E. Then O's rows of key v are I's rows of v, each repeated once per row of E of key v.
- f is min, max, or avg, which don't change when every row of a group is repeated the same number of times. Sum and count scale with E's fan-out, so the pass leaves them.
- g is equated in O, so that no row of O has a NULL key that the join would drop. A semi join on I is allowed only when its source is a subplan of O whose key equals g in O.
- A join pair on the aggregated value (q2's `ps_supplycost = min(...)`) becomes a filter over the window. With such a pair, only an inner join is rewritten.

The multiplicity argument is proved in `formal/WvletFormal/AggregateWindows/` (`part_joined`, `total_scales`, `avg_eq`, `mem_iff`, `covered`). The model does not cover the plan matching.

A shared CTE of O read twice, which is DuckDB's form, was not chosen. It would still aggregate and join back over the CTE rows, and the CTE machinery (`DeferredCTE`) lives in the planner, not in the runtime passes.

## Consequences

- SF10, 10-run one-JVM A/B: H q17 wall 0.58x (CPU 0.63x), H q2 CPU 0.50x (wall flat), DS q92 0.58x, DS q32 0.74x. H q3 and DS q01, whose plans didn't change, stayed at 1.00x and 1.07x.
- The window holds all of O's rows before its parent runs, where the join used to stream O through a hash probe. That is cheap for these queries (O is 61K rows in q17 and 6K in q2). An unfiltered outer side, such as q17 without its part filter, would hold every lineitem row. The pass has no size cap yet; that is a follow-up.
- Correlations that join E to columns of I other than the key (TPC-DS q1, q30, q81, which join customer on the customer key) are left alone, as the multiplicity would differ per row.
