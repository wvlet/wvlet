# Engine: turn a LEFT join into an inner join under a NULL-rejecting condition

## Goals

- Close part of the TPC-DS outer-join gap (DuckDB OUTER_JOIN_SIMPLIFICATION, DataFusion `EliminateOuterJoin`): q93 is 3.24x DuckDB at SF10 because the engine runs `store_sales ⟕ store_returns` (2.9M-row build, 28.8M probes) before the one-row `reason` join.
- Once the LEFT join is an inner join, its inputs join the surrounding inner join tree, so `ReorderJoins` can join `reason` with `store_returns` first and build on the few matching rows.
- First of two PRs for the work item. The second lets a LEFT join commute with inner joins on its preserved side and flips its build side (q80, q40, q77). q49 (`wr_return_amt > 10000` over the null-supplying side) is covered here; q05's LEFT join sits under a UNION, which neither PR crosses.

## Background

- `PushDownFilters` moves a conjunct over a LEFT join into the preserved input when it reads only that input, and otherwise keeps it above (`Pushdown/Rule.lean` proves both, `Necessity.lean` shows why a conjunct can't move into the null-supplying input).
- A conjunct that is never TRUE on a NULL-padded row (`sr_reason_sk = r_reason_sk`, `c > 5`, `c is not null`) removes every padded row, so over it the LEFT join returns the same rows as an inner join.
- In q93 that conjunct is not a filter over the join but a condition of the inner join tree that has the LEFT join as one input (`from ss left join sr on ..., reason where sr_reason_sk = r_reason_sk`).
- q78 filters `sr_ticket_number is null` (anti-join pattern), which is not NULL-rejecting; q75 has no condition over the null-supplying side. This PR does not change them.
- Decorrelated scalar subqueries bind as LEFT joins on an aggregate; a comparison with the scalar (q01, q30, q32, q81, q92, TPC-H q2, q17, q20) rejects its NULLs, so those joins become inner too.

## Design

Lane: Standard (3VL-sensitive optimizer rule), direction pre-approved by the work item; merge held (no merge authorization).

- Lean model first: extend `WvletFormal/Pushdown` with a syntactic `rejectsNull R p` (a comparison with a column of `R` as an operand, IN or NOT IN testing such a column over at least one value, `is not null` on such a column, `and` with one rejecting side, `or` with both) and prove:
  - `p` is never TRUE on a row whose `R` columns are NULL;
  - `filter p (leftJoin on ls rs) = filter p (innerJoin on ls rs)`;
  - an inner join on such a condition over a LEFT join input equals the one over the inner join (the q93 shape).
- `place` for a LEFT join gains the inner-join outcome: a conjunct over the preserved input still moves there; otherwise a NULL-rejecting one makes the join inner and becomes a condition of the new inner join tree; the rest stay above. The fixture gains cases for the inner-join-tree shape with a third table.
- Engine, behind a new toggle `outer_to_inner` (in `OptimizerOptionsTest`):
  - a filter over a LEFT join with a conjunct that rejects NULLs on the right input becomes an `InnerJoins` node over both inputs, with the ON conjuncts and the filter's conjuncts as its conditions;
  - an `InnerJoins` input that is a LEFT join whose right columns a condition rejects (single-input conditions included, q49) is flattened into the tree, to a fixpoint (a flattened join's ON condition can reject another LEFT join input in turn);
  - only in the pass's own walk: `PushDownFilters.filter`, which `ReorderJoins` and `Planner.replan` call after ordering, never creates an `InnerJoins` node that would reach lowering unordered.
- The engine accepts a column under a cast as a rejecting operand (a cast of NULL is NULL); the model has no casts, which the README's known gaps will list.
- RIGHT and FULL joins stay as they are (the bag-order proof for RIGHT needs a permutation lemma); a follow-up if a query needs it.
- Tests: the conformance test replays the extended fixture (filter and tree shapes); a plan test covers the fixpoint; golden plans are regenerated; an SF10 A/B with `WVLET_ENGINE_BENCH_AB=outer_to_inner` on q93, q49, and the decorrelated-scalar queries.

## Alternatives and Why Not?

- **Convert in the binder.** The binder doesn't see conditions pushed from above projections or CTE readers; the pass sees every conjunct where it lands.
- **Push the rejecting conjunct into the null-supplying input and keep the LEFT join.** Wrong: `Necessity.left_null_side_rejecting` shows the padded row comes back.
