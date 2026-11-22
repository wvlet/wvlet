# Engine optimizer M3, PR 1: optimizer passes and Normalize on the runtime plan

## Goals

- Give the runtime plan (`RuntimePlan`) the rule mechanism of the framework plan (section 2): named passes, local rule batches run to a fixpoint, rules-fired counters, and `Optimizer.only(pass)` for per-rule tests.
- Move the first logical rule, OR factoring (`(a and b) or (a and c)` => `a and (b or c)`), onto the IR as the `Normalize` batch, comparing expressions by column ids instead of printed SQL (pain point 4).
- Keep plans unchanged: golden plans, conformance, and benchmarks stay as they are.

## Background

- The runtime binder still chooses join order while binding (`Planner.innerJoinTree` over `Expression` conjuncts, with leaves lowered for estimates). OR factoring has to happen before that, because keys inside OR branches (TPC-H q19, TPC-DS q13/q48) must become equi-keys of the join graph.
- So an IR pass after binding can't take over factoring of inner-join-tree conditions, outer-join ON conditions, or the conditions pushed below a LEFT JOIN until ReorderJoins and PushDownFilters move onto the IR (later M3 PRs).
- The legacy planner stays selectable until M4 and runs `Planner.factorOr` itself, so that code can't be deleted in M3; it goes with the legacy planner in M4.

## Design

Lane: Express — an internal refactor with no change to plans or results, inside the already approved framework design.

- `Optimizer` in `wvlet.lang.engine.runtime`: an ordered list of passes. A pass is a named `RuntimePlan => RuntimePlan`; a `RuleBatch` applies local rules bottom-up until nothing changes, capped at 10 iterations (an error when plans are validated, a warning otherwise).
- Rules are named by the existing `Optimization` values, so `wvlet.engine.optimizer.disabled=factor_or` turns the IR rule off too, and `EngineOptimizerToggleTest` keeps covering it.
- Each run records which rules fired and how often; the planner accumulates the counts so tests can assert that a rule fires (or doesn't).
- With `validatePlans`, `PlanValidator` runs after every pass.
- `Normalize` holds one rule for now, `factor_or`, over the conditions of `Filter` nodes: conjuncts are matched by their bound signature (column ids), with the same factoring and absorption as `Planner.factorOr`.
- The binder no longer factors the conditions of a plain filter (no join below, no subqueries); `Normalize` does it. Join, LEFT JOIN pushdown, and subquery paths keep the Relation-level factoring until their rules move.
- Join leaves are optimized before they're lowered for join-order estimates, so join order sees the same filters as today.
- Tests: per-rule plan tests through `Optimizer.only(Normalize)`, fired counts, and the existing golden plans and conformance suites as the no-change check.

## Alternatives and Why Not?

- **Move join ordering onto the IR first, then factor ORs before it.** It is the right end state, but it needs per-node `Stats` and is a much larger PR; the plan puts it third.
- **Factor ORs on the IR and re-extract equi-keys from join residuals.** Join order would already be fixed without those keys, so plans would regress (q13/q48).
