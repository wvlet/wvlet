# Engine optimizer M3c, PR 2: PushDownFilters on the runtime plan

## Goals

- Push filter conjuncts down the runtime plan in a `PushDownFilters` pass between `Normalize` and `ReorderJoins`: through projections, aggregates (key-only conjuncts), and sorts without a limit, into the preserved side of outer joins, and into inner join trees, inferring equalities transitively there.
- Move the two remaining compiler-expression rewrites of outer joins onto the IR: the LEFT JOIN filter pushdown in `RuntimeBinder.filter` (`planner.conjuncts`), and the OR factoring and equi-key extraction of outer-join ON conditions (`Planner.joinOn`).
- Keep the golden plans unless a change is justified, with benchmark evidence.

## Background

- The binder binds an outer join through `Planner.joinOn` over lowered inputs: it optimizes each input first, because `joinOn` reads operator schemas and estimates to choose keys and the build side. A filter over a LEFT JOIN is split by the planner before binding, and the conjuncts over the left input bind under the left input, where an inner join tree takes them as conditions.
- The legacy planner pushes filters nowhere else (besides inner join trees and row-group filters), and `EnginePlanGoldenTest` checks both planners against the same files.
- The IR has no window, set-operation, or CTE nodes yet: those are `Legacy` leaves, so this PR can't push through them.

## Design

Lane: Express. The direction (scope, gates) was given in the launch prompt.

- An outer ON join (LEFT, RIGHT, FULL, without subqueries in ON) binds as a `Join` over unoptimized inputs, with its whole ON condition bound as the residual and no keys. `FactorOr` factors the residual's conjuncts, and `PushDownFilters` moves equalities between the two inputs into hash keys, in their written order, as `joinOn` does. `ReorderJoins`, which walks bottom-up after the inputs are ordered, then chooses the outer build side from `Stats.rows` (today's `swapsOuterBuild`).
- `Filter` carries literal counts by bound signature, as `JoinCondition` does, so that a conjunct pushed into an inner join tree keeps today's selectivity. The binder records them for every filter it binds.
- A filter over an outer join without subqueries binds as a `Filter` over the bound join; the pass pushes its conjuncts over the left input of a LEFT JOIN (right of a RIGHT JOIN) down that side. With subqueries, the binder keeps today's path until Decorrelate.
- `PushDownFilters` walks top-down. A conjunct passes a projection by substituting the projected expressions (when they are deterministic), an aggregate when it reads only grouping keys, and a sort without a limit. It stops at limits, full joins, `Legacy` leaves, and filters. In an inner join tree it becomes a condition; there, `a = b` and `a = constant` add `b = constant` when `a` and `b` are columns of the same type.
- Deleted from the runtime binder: the outer-join call into `Planner.joinOn` and the LEFT JOIN `planner.conjuncts` split (both kept for conditions with subqueries and for the legacy planner until M4).
- `ReorderJoins` pushes each input's single-input conditions through the same walk, so that a second run of the passes over a subtree the binder optimized early (aggregation inputs) leaves it unchanged.
- Golden plans: the runtime planner now differs from the legacy one on 13 of 121 plans (11 only move a filter below a rename projection; q49 filters the preserved side of its LEFT JOINs at the scan; q70 prunes one more column). `spec/engine-plans/` keeps the runtime planner's plans, and `spec/engine-plans/legacy/` the legacy planner's plans where they differ, so the legacy planner stays checked until M4 deletes it.
- Tests: `Optimizer.only(PushDownFilters)` cases for each push, key extraction, and transitive equalities; golden plans, conformance (spec/basic both planners, TPC-H, TPC-DS) and the toggle test for the rest. The pass is named `push_down_filters`, so the toggle test runs spec/basic with it off.

## Alternatives and Why Not?

- **Keep binding outer joins over lowered inputs and push only into what's bound.** The inputs are already ordered by then, so a pushed conjunct can't reach an inner join tree's order.
- **Count literals on bound constants for pushed conjuncts.** Changes selectivity for folded casts and scalar subquery results (the separate, plan-changing PR in the work item).
- **Push into CTE bodies now.** CTEs are `Legacy` leaves; it needs a CTE node on the IR, so it is a follow-up.
