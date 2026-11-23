# Engine optimizer M5: exact row counts of materialized shared CTEs feed join order

## Goals

- Start M5's adaptive re-planning (framework plan, sections 4 and 7): a shared CTE the engine computes anyway gives `ReorderJoins` its exact row count, in place of the fixed 1e6 estimate.
- Keep the first PR small: one materialized form (shared CTEs, `CTERef`), one statistic (rows), one re-planned scope (the root plan's joins).

## Background

- A CTE read more than once is a `DeferredCTE`. Its body is bound at the first read. It is lowered at the end of the outermost `Planner.plan` with the union of the readers' columns and the rows their filters agree on (ADRs 2026-11-22 shared-CTE deferred pruning and reader filters). It runs on first read at execution time.
- `Stats.rows(CTERef)` and `CardinalityEstimator.rows` over a CTE scan both return 1e6, so join order and build sides treat every shared CTE as a large table. At SF1 this flips decisions that SF0.1 golden plans don't show.
- A first plan is needed before the body can run: the readers' columns and filters come from it. Because a CTE's row count is fixed, join order never changes which columns or filters a reader has, so the second plan needs the same body result.
- 20 TPC-DS golden plans read a shared CTE. The binder already optimizes and lowers some subtrees while binding (aggregation inputs, pivots, subquery inputs), so their joins are ordered `Join` nodes in the bound root, ordered against the 1e6 estimate. A second run of the passes can't change them.

## Design

- Lane: Standard by size, but the launch prompt already set the direction (the M5 item's first entry) and authorized merge, so this proceeds without a direction card.
- After the outermost plan finalizes its shared CTEs, `Planner.plan` checks the root's bound runtime plan for the joins `ReorderJoins` still decides (`InnerJoins` graphs, and outer hash joins, whose build side it picks) with a `CTERef` below them. If there are any, it computes those CTEs and records their exact row counts. Joins the binder ordered while binding keep their order (follow-up).
- `Stats.rows(CTERef)` returns the recorded count, and the CTE's table source reports it as `estimatedRows`, so `CardinalityEstimator` over the lowered scan reads the same number (keeps the two estimators in step). Without a count, both still say 1e6.
- The root's bound plan is optimized again with the same passes; when the optimized plan differs, it is lowered and physically optimized again, and that operator replaces the first one. Otherwise the first operator is kept.
- Re-lowering creates new CTE readers after the body was filtered. That is allowed only when the new reader's filter equals a filter some reader already reported, since then it implies the conditions pushed into the body; anything else still fails with UNEXPECTED_STATE.
- Join order shouldn't change which columns a reader reads, but this is checked rather than assumed: if a new reader's scan reads a column the result dropped, or a reader is refused, the first operator is kept (tests, which validate plans, raise the error instead).
- The second run uses its own optimizer, so `firedRules` counts each rule of the root plan once.
- A new toggle `replan_materialized_ctes` turns the step off. Off, the plans and the time when CTEs run are as before.
- Plan-time effect: `explain` now runs the shared CTEs that joins read, as it already runs uncorrelated scalar subqueries. A CTE that execution would only partly read (under a LIMIT, behind an empty build side) is now computed in full. An error in such a CTE body now shows up during planning, not during execution.
- Golden plans that change are regenerated, with SF1 benchmark evidence (toggle A/B on one build, alternating rounds, unchanged-plan controls).
- Tests: a unit test showing a small CTE joined with a large table gets the exact count and flips the build side; the toggle test; conformance over spec/basic, TPC-H, TPC-DS.

### Follow-ups recorded on the M5 item

- Joins ordered at bind time (aggregation and subquery inputs) with a `CTERef` below: re-planning them needs those subtrees kept unordered in the bound plan, or their inner `Join` trees flattened again.
- CTE bodies that join other shared CTEs are ordered at bind time with 1e6; re-planning them needs the inner CTE computed before the outer body is lowered.
- Exact per-reader stats: rows after the reader's filter, and distinct counts of join keys, both computable from the materialized batches.
- Other materialized forms: plan-time IN/EXISTS subquery results and grouping-sets inputs (`SharedInput`).

## Alternatives and Why Not?

- Compute the CTE when it is first read, at bind time: loses the readers' column pruning and filter pushdown, which landed for 8 + 7 TPC-DS plans.
- Estimate CTE rows from the body's `Stats` instead of computing them: still guesses (aggregations pass their input's rows through), and doesn't start the adaptive path M5 asks for. It could also be useful as the fallback when the step is off.
- Re-bind the whole query after materialization: binding calls back into the planner (CTE registration, plan-time subqueries), so a second bind repeats those side effects.
