# Engine optimizer M3c, PR 11: Decorrelate correlated EXISTS / NOT EXISTS / IN / NOT IN on the runtime plan

## Goals

- Continue framework pass 2 (Decorrelate): filter conjuncts `[NOT] EXISTS (sub)` and `x [NOT] IN (sub)` over a correlated subquery bind as runtime-plan semi/anti joins instead of `Legacy` nodes, so the subquery body is on the IR (pushdown, pruning, scan filters reach into it).
- Build the same operators the legacy planner builds, so golden plans change only where a filter, join, or column set now moves.
- Keep NULL semantics of NOT IN / NOT EXISTS exact.

## Background

- PR 9 (#2279) bound the uncorrelated forms on the IR. A conjunct list with any correlated subquery still goes through `Planner.applyConjuncts` and comes back as one `Legacy` node.
- The planner decorrelates a subquery `[Project | Distinct] Filter(child, cond)` (`planCorrelated`): the conjuncts that bind in the child filter it, and the rest are correlated conditions. `correlationKeys` splits those into `inner = outer` equalities, which become hash keys, and a residual over outer ++ inner, with inner names resolving first.
- EXISTS / NOT EXISTS become a semi / anti join on the keys (a nested loop join when there are none), with the build side chosen by `swapBuild` from estimates. IN becomes a semi join on `(target, keys)` over the projected select item.
- Correlated NOT IN is an anti join on `(target, keys)`, which is not SQL's semantics. It lets a row through when its target is NULL, or when its correlated group holds a NULL, and the group is not empty. SQL returns NULL for both, so the row should be filtered out. No TPC query uses correlated NOT IN.
- TPC queries whose conjunct lists this PR moves onto the IR are TPC-H q4, q21, q22 and TPC-DS q16, q69, q94. q69's parenthesized `not exists ... and not exists ...` splits into top-level conjuncts. TPC-DS q10 and q35 also have a top-level correlated EXISTS, but another conjunct in the same list puts EXISTS under OR, so the whole list stays `Legacy` until part 4.

## Design

Lane: Express. An internal planner change. The direction and gates came with the launch prompt. The NOT IN fix is a correctness fix for an existing bug.

- **One shared decorrelation.** The planner's correlated EXISTS / IN code becomes one function that is generic over the plan builder (`JoinPlanBuilder & AggPlanBuilder`, as `joinOn` and `aggregation` already are). The planner calls it with operators. The runtime binder calls it with IR nodes, binding the subquery body through itself. Both planners then build the same joins, keys, residuals, and build sides by construction.
- **Binding.** `plans.subqueryJoins` accepts correlated `[NOT] EXISTS` / `[NOT] IN` conjuncts next to the uncorrelated ones, applying them in conjunct order as `applyConjuncts` does. Conjunct lists with subqueries under OR / NOT / CASE, correlated scalar subqueries, or a subquery in an IN target stay `Legacy` (part 4). An unsupported body shape fails with the same error as before.
- **Where it applies.** It applies everywhere the binder already calls `plans.conjuncts`: a filter over a non-join, and conditions with subqueries over an ordered inner join tree. Correlated joins stay above the tree. Placing them inside it is part 2b's cost-based placement.
- **Exact correlated NOT IN, on both planners.** `x NOT IN (select y ... where corr)` becomes an anti join on the correlation keys, with the residual `corr_rest and (x = y or x is null or y is null)`. That is the standard rewrite to NOT EXISTS: a row passes only when no row in its group makes `x <> y` other than TRUE. The residual is built from bound expressions (the target in the outer scope), so a target whose name matches an inner column still reads the outer one. It loses `x` as a hash key, so it can be slower, but it is exact.
- **Toggle.** `decorrelate_correlated` (on by default, and only effective with `decorrelate`). Off sends correlated conjuncts back through the planner, which gives main's plans byte for byte, for A/B on one build. The only exception is correlated NOT IN, which is now exact on both planners and has no golden plan.
- **Plan changes.** These come from the subquery body now being optimized on the IR: local filters pushed into scans, pruned columns, and parallelization together with the outer plan, as in PR 9. Golden plans show which TPC queries move. Changed queries are benchmarked toggle off/on at SF1, with their SF1 plans checked.
- **Tests.** `EngineDecorrelateTest` covers each correlated form, both planners giving the same rows, no `Legacy` nodes, and NOT IN against NULL targets and NULL group values, checked against DuckDB's answers.

## Result

- With the toggle off, golden plans equal main's byte for byte on both planners. This was checked by regenerating them with it disabled.
- 2/121 golden plans changed: TPC-H q4 and q22. The plain conjuncts now push below the semi / anti join. In q4 the date filter moves below the RIGHT SEMI join and becomes a row-group filter on `orders`. In q22 the phone and balance filter on `customer` runs before the RIGHT ANTI join. The legacy-planner plans (equal to main's) are added under `spec/engine-plans/legacy/tpch/`. TPC-H q21 and TPC-DS q16, q69, q94 keep their plans. The same two queries are the only ones whose plans change at SF1.
- SF1 A/B on one build, toggle off/on, 4 alternating full-suite rounds: q4 160 → 31 ms (−81%), q22 −17%, both consistent in every round. TPC-H unchanged-plan controls −2%. TPC-DS, whose plans are all unchanged, +8% (drift).

## Follow-ups (recorded on the item)

- Correlated semi / anti joins over an inner join tree stay above the ordered tree. Placing them is part 2b's cost-based placement.
- Correlated NOT IN loses the target as a hash key to stay exact. A null-aware anti join could keep it.

## Alternatives and Why Not?

- **A separate IR copy of `planCorrelated` / `correlationKeys` in the binder.** It would duplicate about 150 lines whose build-side and key-casting choices have to match exactly. A generic function keeps one copy.
- **Keep correlated NOT IN `Legacy` (or inexact).** The IR would then preserve a wrong answer. The fix is small and shared, and no golden plan uses the form.
- **Place correlated semi joins inside `InnerJoins`.** That changes join order, so it needs selectivity estimates. It belongs to part 2b.
