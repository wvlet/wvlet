# Engine optimizer M2b, PR 2: joins in the logical IR

Part of milestone M2 (`plans/2026-11-22-engine-optimizer-m2-logical-ir.md`). PR 2 of the M2b split: Join here, then Aggregate, then the default planner switch.

## Goals

- Joins become logical nodes with column ids, so that M3 can push filters through joins and reorder join graphs on the IR instead of on lowered operators.
- No plan changes: both planners still match every golden TPC-H and TPC-DS plan.

## Background

- Today every `Join` is a `Legacy` leaf on the logical path: the binder hands it to `planJoinTree`, which plans the leaves, builds the `JoinGraph`, and runs `JoinOrderOptimizer`, all over operators.
- Join ordering reads cardinality estimates from lowered operators (`CardinalityEstimator`), and per-node `Stats` on the IR come only in M3. So in M2 the order must still be chosen from lowered leaves.
- Planner side effects (shared CTEs, hidden names, scalar subquery caches) happen while planning leaves and conditions. Plans stay identical only if the binder does this work in the planner's order.

## Design

Lane: Express. An internal refactor behind `wvlet.engine.planner=logical`, whose default stays `legacy`; the golden plans prove nothing changes.

- Add one binary node `Join(kind, left, right, leftKeys, rightKeys, residual, buildLeft)` with kinds inner, left, right, full, and cross. Equi keys and the residual are `BExpr`s over the inputs' ids. Lowering emits `HashJoinOp` when there are keys and `NestedLoopJoinOp` otherwise, through the same helper the planner uses.
- `buildLeft` is decided at bind time, as the planner decides it today: from the estimated rows of the lowered inputs and gated by `SwapBuildSide`. Lowering only applies it (today's `swappedHashJoin`, with Left and Right flipped).
- Inner-join trees: the planner's `planJoinTree` becomes generic over what it builds, and the binder calls that same code with logical leaves. It then plans the leaves, checks the conditions for subqueries, builds the `JoinGraph`, and runs the multi-start greedy in exactly today's order. So planner side effects (CTE sharing, subquery planning in `isCorrelated`, hidden names) happen in the same sequence.
- The binder's builder carries each partial tree as a pair: the logical node and its operator. `JoinGraph` and `JoinOrderOptimizer` read estimates and schemas from the operator half, so the chosen order and build sides are unchanged, and nothing is lowered again per candidate.
- Conditions go through the planner's `applyConjuncts` over the operator half, as today. When it returns a plain filter, the binder records a logical `Filter` with the bound condition. Uncorrelated scalar subqueries (e.g., `d_month_seq = (select ...)`) are already constants in that condition. When it adds subquery joins (EXISTS, IN, correlated scalars), the result is a `Legacy` node over the lowered join.
- That `Legacy` node is a new pattern: a legacy relation planned over an input the binder already bound. It stays correct because the operators are the planner's own, but those joins drop out of the IR and the validator until Decorrelate (M3). M3's `PushDownFilters` won't see through them.
- Semi and anti joins exist only inside the planner's subquery decorrelation (the parser has no such join type), so they stay inside those `Legacy` nodes until M3.
- Outer joins with `ON` or `JoinOnEq` conditions bind as logical `Join`s. A filter over a left join pushes conditions on the preserved side below the join, binding `j.left` twice in the planner's order: once for its schema, once under the pushed filter.
- These stay `Legacy` and are routed before any input is bound: ASOF joins, `USING` and `NATURAL` joins, and joins with a lateral `unnest(...)` on the right (all M4).
- Tests: the golden plans on both planners; spec/basic with the plan validator on the logical path; and binder tests that a join-only query, an outer join, and the join under a TPC-H aggregate bind to `Join` nodes.
- Benchmarks: the logical path is off by default here, so the benchmark gate runs at the default-switch PR, as the work item says. This PR checks the planning cost of the pair builder by timing golden-plan planning on both planners.

## Alternatives and Why Not?

- Order joins at lowering time: conditions and leaves would be planned in a different order, which changes hidden names and CTE sharing in the golden plans.
- Plan the tree with the operator optimizer, then convert the operators back into logical nodes: this depends on the operator shapes the optimizer happens to produce.
- Re-lower each partial tree for every estimate: simpler, but planning time grows with the cube of the input count (q64 has about 18 inputs).
- Move join ordering onto `Stats` now: that is M3's `ReorderJoins`, and it changes estimates, so plans would drift in a PR meant to change none.
