# Engine optimizer M3c, PR 10: Decorrelate part 2, semi joins on their input inside inner join trees

## Goals

- An uncorrelated `x [NOT] IN (subquery)` conjunct over an inner join tree whose `x` reads a single input becomes a semi / anti join on that input, inside `RuntimePlan.InnerJoins`, instead of a join over the ordered tree (`plans.conjuncts(lowered(InnerJoins), ...)`). Join order then joins the reduced input, and the input's single-input filters push below the semi join.
- Keep NOT IN's NULL semantics exact and lower to the operators part 1 builds (`JoinOrderOptimizer.semiJoinOp`).

## Design

Lane: Express. An internal planner change with no user-facing behavior change; the direction and gates came with the launch prompt.

- **Which conjuncts move.** In `RuntimeBinder.innerJoins`, a condition with a subquery moves onto input `i` when it is `[NOT] IN` over an uncorrelated subquery, its compared value has no subquery, and it reads input `i` alone (`Planner.referencesInput`, as for plain conditions). Every other condition with a subquery (EXISTS, correlated forms, IN over several inputs) still applies over the ordered tree, as today.
- **Binding.** The input becomes `Join(input, sub, Semi | Anti, ...)`, which outputs the input's columns, so the `InnerJoins` output and its condition bindings keep their ids. IN builds on the subquery unless it is estimated as much larger than the input (`swapBuild` over `Stats.rows` of the input and the optimized subquery's rows, as part 1 decides it over the optimized input). NOT IN materializes the subquery at plan time, as part 1 does: a NULL gives `Filter(false)`, otherwise an `Anti` join that never swaps with the values, then `x is not null`. The key and NOT IN helpers are shared with part 1's path.
- **Estimates.** `Stats.rows` of a semi join is still its left input's rows, as `CardinalityEstimator` counts it. The join order may change only through the build side the input now has; an estimate for semi-join selectivity is a separate plan-changing step (follow-up).
- **Toggle.** `place_semi_joins` (with `decorrelate`): off, the semi joins stay above the ordered tree, so A/B runs on one build.
- **Legacy planner.** Unchanged: it applies these conditions over its ordered tree. Changed queries without a `spec/engine-plans/legacy/` file get one.

## Gates

Conformance on both planners (spec/basic, TPC-H 22/22, TPC-DS 99/99), engineJS/Native compile, golden plan diffs with SF1 A/B (toggle off/on, alternating rounds, unchanged-plan controls).

## Alternatives and Why Not?

- **Decide the semi-join build side in ReorderJoins.** NOT IN's anti join must not swap; deciding in the binder keeps part 1's rule and needs no new flag on `Join`.
- **Lower the input first to estimate it** (as part 1 does for a filter over a non-join). A nested join tree in the input would then be ordered before its parent's filters reach it (notes from #2271); `Stats.rows` over the bound input gives the same row count for scans without it.
