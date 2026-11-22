# Engine optimizer M3c, PR 1: factor ORs of inner join conditions in Normalize

## Goals

- Move OR factoring of inner join tree conditions (ON conditions inside the tree and filters over it) from the planner's compiler-expression rewrite into the `FactorOr` rule of the `Normalize` pass, over bound expressions.
- Keep every plan the same: golden plans under `spec/engine-plans/` must not change.
- Next step toward retiring `Planner.conjuncts` factoring (section 7 of `plans/2026-11-22-engine-optimizer-framework.md`).

## Background

- The runtime binder builds `RuntimePlan.InnerJoins` from `Planner.flattenJoins` and `Planner.conjuncts`, which factor `(a and b) or (a and c)` into `a and (b or c)` before binding, so the common equality can become a hash join key.
- `FactorOr` already factors `Filter` conditions on the runtime plan (#2266), and `ReorderJoins` now orders `InnerJoins` after `Normalize` (#2268), so the factoring can happen on the IR instead.
- Each `JoinCondition` carries `literals`, the literal count decided on the compiler expression (`CardinalityEstimator.literalCount`), for its selectivity. A factored-out conjunct is a sub-expression of the bound OR, so its literal count has to survive the rewrite. A bound `Const` can't tell a literal from a folded cast or a scalar subquery result, so counting on the bound side would change plans.
- Two other join paths still factor on compiler expressions and need IR rules that don't exist yet: equi-key extraction from outer-join ON conditions (`Planner.joinOn`), and pushing a filter's conjuncts to the left input of a LEFT JOIN. They stay as they are in this PR.

## Design

- Lane: Express. Internal refactor with no intended plan change, checked by the golden plans and conformance suites.
- The binder binds inner join tree conditions unfactored: `flattenJoins` takes the splitter to use, and the filter over an inner join tree is split without factoring.
- `FactorOr` also rewrites `InnerJoins`: each condition is replaced in place by its factored conjuncts, so condition order (and the greedy join search's tie-breaking) stays as today.
- `JoinCondition` keeps literal counts by bound signature for the condition and its `and`/`or` sub-conditions, recorded when the binder binds it by walking the compiler and bound expressions together. Factored conjuncts look their count up by signature. `literals` stays as the condition's own count for `Stats.selectivity`.
- Conditions with (non-scalar) subqueries stay outside the IR until Decorrelate, so the binder still factors those through the planner before splitting them off; the same holds for the legacy paths (lateral unnest, LEFT JOIN pushdown, outer-join ON).
- Tests: `EngineNormalizeTest` cases for factoring an inner join tree condition into a join key, with the literal count kept; golden plans, TPC-H/TPC-DS conformance and spec/basic unchanged.

## Alternatives and Why Not?

- Count literals on bound expressions in `Stats`: changes selectivity for folded casts and scalar subquery results (the separate, plan-changing PR noted in the work item).
- Move the outer-join ON and LEFT JOIN pushdown factoring now: needs equi-key extraction and filter pushdown on the IR first (PushDownFilters), which is a later item.
