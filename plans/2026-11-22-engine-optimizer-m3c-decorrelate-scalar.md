# Engine optimizer M3c, PR 12: Decorrelate correlated scalar subqueries and EXISTS marks on the runtime plan

## Goals

- Finish framework pass 2 (Decorrelate): filter conjuncts holding a correlated scalar subquery (`l_quantity < (select 0.2 * avg(...) where l_partkey = p_partkey)`) or a correlated EXISTS under OR / NOT / CASE bind as runtime-plan nodes instead of making the whole conjunct list `Legacy`.
- Build the same operators the legacy planner builds, so golden plans change only where a filter, join, or column set now moves.
- Keep SQL NULL semantics exact. A correlated scalar subquery over an empty group gives the subquery's value over no rows: NULL for most aggregates, 0 for `count`. An EXISTS mark is never NULL.

## Background

- `Planner.applyConjuncts` turns top-level [NOT] EXISTS / [NOT] IN into semi / anti joins. It then binds each other conjunct with `bindWithSubqueries`, which rewrites a correlated scalar subquery into a column of a left join with the subquery grouped by its correlation keys (`decorrelateScalar`). It also rewrites a correlated EXISTS inside an expression into a boolean column: a left join with the distinct correlation keys, marked `key is not null` (`decorrelateExists`).
- On the runtime plan, `plans.subqueryJoins` (#2279, #2281) binds the top-level forms. A conjunct list in which any other conjunct holds a subquery goes back through `applyConjuncts` and comes back as one `Legacy` node. TPC queries affected: TPC-H q2, q17, q20 (whose IN body holds the scalar subquery), and TPC-DS q1, q6, q10, q30, q32, q35, q41, q81, q92.
- **COUNT bug.** `decorrelateScalar` gives NULL for a group with no rows, which is wrong for `count`. Today `where (select count(*) from supplier where s_nationkey = n_nationkey and s_acctbal > 9900) = 0` returns 0 rows, but DuckDB returns 24. TPC-DS q41 uses `count(*) > 0`, where NULL and 0 give the same answer.

## Design

Lane: Express. This is an internal planner change. The direction and gates came with the launch prompt, and the COUNT fix is a correctness fix for an existing bug.

- **One shared decorrelation.** As `correlatedJoin[P]` did in #2281, `bindWithSubqueries`, `decorrelateScalar`, and `decorrelateExists` become generic over the plan builder (`JoinPlanBuilder & AggPlanBuilder`), with the body planned through `planCorrelated[P]`. The planner calls them with operators. The runtime binder calls them with IR nodes (`Aggregate`, `Project`, a `Left` `Join` building on the subquery, and the mark `Project`). Both planners then build the same operators by construction.
- **Binding.** `plans.subqueryJoins` takes every conjunct list whose IN targets have no subquery. Top-level subquery conjuncts become joins in conjunct order. Every other conjunct goes through the shared `bindWithSubqueries` over the joined plan. A filter over the result and a projection back to the input's columns follow, as in `applyConjuncts`. Uncorrelated subqueries inside expressions stay what the binder makes of them today (constants).
- **Exact empty groups (both planners).** When the subquery's value over no rows is not NULL (it is evaluated at plan time, like an uncorrelated scalar subquery), the joined value becomes `case when key is null then <empty value> else value end`, using a right-side key that is non-NULL exactly on matched rows. Otherwise the plan is unchanged, which covers avg, sum, min, and max.
- **Toggle.** `decorrelate_scalar` (on by default, and only effective with `decorrelate`). Turning it off sends these conjunct lists back through the planner, which gives main's plans byte for byte, so the change can be A/B tested on one build. The COUNT fix applies with the toggle on or off.
- **Plan changes.** These come from the subquery bodies and the outer inputs now being optimized on the IR: pushdown, pruning, scan filters, and parallelizing together with the outer plan. Golden plans show which TPC queries move. Changed queries are benchmarked toggle off/on at SF1, with their SF1 plans checked.
- **Tests.** `EngineDecorrelateTest` covers a correlated scalar subquery, the COUNT case, EXISTS under OR and under NOT, no `Legacy` nodes, and the same rows on both planners. Expected counts come from DuckDB.

## Result

- With the toggle off, golden plans equal main's byte for byte on both planners. The one exception is TPC-DS q41, whose plan gains the COUNT fix's `case` projection on both planners. This was checked by regenerating the plans with the toggle disabled.
- 4/121 golden plans changed. Their legacy-planner plans are added under `spec/engine-plans/legacy/` (TPC-H q20 already had one).
  - TPC-DS q41: the `i_manufact_id` range filter moves below the left join and becomes a row-group filter on `item`. The COUNT fix's `case` merges into the filter.
  - TPC-DS q10, q35: the EXISTS marks now merge into the filter (`#9 or is_null(#10, true)`). The projection back to the input's columns is a separate identity `Project`.
  - TPC-H q20: `ReorderJoins` picks the build side of the scalar subquery's left join from estimates, as it does for other outer joins on the IR. It now builds on the (`partsupp` semi `part`) outer side and probes with the grouped `lineitem` (RIGHT join).
  - TPC-H q2, q17 and TPC-DS q1, q6, q30, q32, q81, q92 move onto the IR with unchanged golden plans.
- At SF1, the plans of TPC-DS q32 and q92 also change: they build the left join on the outer side, as q20 does.
- SF1 A/B on one build, toggle off/on. A full run took 4 alternating rounds, then the changed queries took 6 more rounds of 7 runs each. Changed queries, on/off median: H q20 0.82, DS q10 0.96, q35 0.94, q32 1.04, q41 1.02, q92 0.92. Unchanged-plan controls in the same rounds ranged from 0.74 to 1.08. There are no regressions, and no errors in any round.

## Follow-ups (recorded on the item)

- A dedicated mark join, or a semi join with a mark output, would avoid the distinct aggregation and the identity projection. It folds into part 2b or M5.
- A correlated scalar subquery still needs equality correlations only (no residual), as before.

## Alternatives and Why Not?

- **A separate IR copy of the scalar and mark decorrelation.** Its key casting and schemas would have to match the planner's exactly. A generic function keeps one copy.
- **A mark join operator.** A left join with the distinct keys is what the planner builds, and it keeps golden plans stable. A dedicated mark join, or a semi join with a mark output, belongs with cost-based placement (part 2b) or M5 operator work.
- **`coalesce(value, <empty value>)` for the COUNT fix.** It is wrong when a matched group's value is NULL but its empty value is not (for example, `case when count(*) > 0 then null else 1 end`).
