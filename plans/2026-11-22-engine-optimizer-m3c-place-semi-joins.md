# Engine optimizer M3c, PR 10: Decorrelate part 2, semi joins on their input inside inner join trees

## Goals

- An uncorrelated `x IN (subquery)` conjunct over an inner join tree whose `x` reads a single input becomes a semi join on that input, inside `RuntimePlan.InnerJoins`, instead of a join over the ordered tree (`plans.conjuncts(lowered(InnerJoins), ...)`). Join order then joins the reduced input, and the input's single-input filters push below the semi join.
- Lower to the operators part 1 builds (`JoinOrderOptimizer.semiJoinOp`), and place a semi join only where it can't make the plan worse.

## Design

Lane: Express. An internal planner change with no user-facing behavior change; the direction and gates came with the launch prompt.

- **Which conjuncts move.** In `RuntimeBinder.innerJoins`, a condition with a subquery moves onto input `i` when it is `IN` over an uncorrelated subquery, its compared value has no subquery, it reads input `i` alone (`Planner.referencesInput`, as for plain conditions), and the subquery is estimated no larger than the input. The semi join then builds on the subquery and filters the input as it streams.
- **What stays above the ordered tree.**
  - IN over a larger subquery joins the tree, reusing the subquery already lowered for the estimate; the build side is decided over the tree as in part 1. Placed on the input, it would build on the subquery (or, swapped, on the whole input) before the tree's joins reduce the input. TPC-DS q95's `ws_wh` CTE (unknown size, estimated at 1e6 rows) over `web_sales` ran 7–10× slower placed.
  - NOT IN, which rarely removes many rows: placed on the input it mostly adds probes (TPC-H q16, +4% to +23%).
  - EXISTS, correlated forms, and IN over several inputs, as today.
- **Binding.** The placed input becomes `Join(input, sub, Semi, buildLeft = false)`, which outputs the input's columns, so the `InnerJoins` output and its condition bindings keep their ids. The keys (`inKeys`) and NOT IN materialization (`notInValues`) are shared with part 1's path; `plans.inJoin` is part 1's IN join, reused for the IN conditions kept above the tree.
- **Estimates.** `Stats.rows` of a semi join is still its left input's rows, as `CardinalityEstimator` counts it. Join order changes only through the placed input's shape; a semi-join selectivity estimate is a separate plan-changing step (follow-up).
- **Toggle.** `place_semi_joins` (with `decorrelate`): off, the semi joins stay above the ordered tree (golden plans equal main's byte for byte), so A/B runs on one build.
- **Legacy planner.** Unchanged: it applies these conditions over its ordered tree. The changed queries get files under `spec/engine-plans/legacy/`.

## Result

- 5/121 golden plans changed (TPC-DS q33, q56, q58, q60, q83): the `item` (or `date_dim`) input semi-joins its filtered copy before it joins the fact table.
- SF1 A/B on one build (toggle off/on, alternating rounds, machine load 7–11): q58 −23% to −35%, q83 −16% to −26% in every set; q33, q56, q60 within the drift of unchanged-plan controls (q96 drifted +33% in the same rounds). Unchanged-plan suites: TPC-H total 0.94, TPC-DS total 0.99 (on/off). q95, q14, q70, and TPC-H q16/q18/q20 keep main's plans.

## Follow-ups (recorded on the item)

1. Cost-based semi-join placement: estimate semi-join selectivity in `Stats` and let ReorderJoins place a semi join (q18 gained 16% with its RIGHT SEMI on `orders` placed, while q95/q70 regressed).
2. Inner join output pruning: a projection after the join tree's last join (q16's placed anti join carried unused key columns into its aggregation).

## Alternatives and Why Not?

- **Place every single-input [NOT] IN.** Measured: q95 ×7, q70 +28%, q14 +13%, q16 +23%, against q18 −16%, q58 −32%, q83 −26%. Without semi-join estimates in join order, placement has to be limited to the cases that don't lose.
- **Decide the semi-join build side in ReorderJoins.** NOT IN's anti join must not swap; deciding in the binder keeps part 1's rule and needs no new flag on `Join`.
- **Lower the input first to estimate it** (as part 1 does for a filter over a non-join). A nested join tree in the input would then be ordered before its parent's filters reach it (notes from #2271); `Stats.rows` over the bound input gives the same row count for scans without it.
