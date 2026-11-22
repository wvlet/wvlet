# ADR: The engine's logical IR grows inside the recursive planner

Date: 2026-11-22 · PR #2261

## Context

Milestone M2 of the engine optimizer framework ([2026-11-22-engine-optimizer-framework.md](2026-11-22-engine-optimizer-framework.md)) adds a logical IR with column ids between the compiler's `Relation` and the physical `Operator`s. The recursive `Planner` covers about 50 relation kinds and is being edited by other sessions at the same time, so the IR can't replace it in one PR. M2 must also change no plan: the golden TPC-H and TPC-DS plans are the proof that a second planning path is equivalent.

## Decision

- `Planner.planRelation` dispatches on `wvlet.engine.planner`. With `logical`, it binds the relation with `LogicalBinder`, lowers the `LogicalPlan`, and returns the operators. With `legacy` (the default until the IR covers joins and aggregations), it runs the old recursion, now named `planLegacy`.
- A relation the binder doesn't cover becomes a `Legacy(op)` leaf, planned at bind time by `planLegacy`. Its own inputs go back through `planRelation`, so scans, filters, and projections under joins and aggregations are planned through the IR too (a strangler pattern that interleaves both paths in one plan).
- The binder routes each relation by the planner's own tests, in the planner's case order, and binds inputs in the planner's order. It never tries to bind and then falls back. Planner state changes in the same order on both paths (registered and shared CTEs, hidden column names, scalar subquery caches), and so the lowered plans are identical.
- A bound expression (`BExpr`) is the executable `VExpr`, with column ids in place of positions in its `ColRef`s. `VExprs.remap` converts in both directions. This relies on every position a `VExpr` reads being among its `children`; lambda bodies bind over their own scope, so they don't break this.

## Consequences

- `EnginePlanGoldenTest` checks both planners against the same golden files. A routing mistake in the binder shows up as plan drift there.
- Each later PR moves one relation kind (or one rewrite) from `planLegacy` into the binder and passes. Until a kind moves, its planner code is the only implementation, so concurrent planner edits keep working.
- Expressions get no new tree. A rule that needs more structure than ids and `VExpr` shapes (for example, a decorrelation that rewrites subquery expressions) will need a richer `BExpr` at that point.
- Until the default switches, the logical path runs only in tests and when the property is set. `EngineBasicSpecLogicalTest` and the golden test keep it working.
- Joins (PR #2262) bind as logical `Join` nodes, but join order is still chosen over operators: the binder carries each partial join tree with its lowered operators, and `JoinOrderOptimizer` and `planJoinTree` are generic over what they build. Conditions with subqueries are applied by the planner over the lowered join and come back as a `Legacy` node. This is the one place where a legacy relation is planned over an input the binder already bound. It is correct, because the operators are the planner's own, but those joins are invisible to logical passes until decorrelation moves to the IR (M3).
- Aggregations follow the same pattern: `planAggregation` is generic over what it builds, and the binder gets logical `Aggregate`, HAVING `Filter`, and `Project` nodes from it. Grouping sets and windows over aggregates are built by the planner over the bound input and come back as `Legacy` nodes.
- Renamed after M2b: the IR is `RuntimePlan` in `wvlet.lang.engine.runtime` (binder `RuntimeBinder`, switch value `wvlet.engine.planner=runtime`). `LogicalPlan` was already the compiler's relation type (`wvlet.lang.model.plan.LogicalPlan`), so the binder had to hide one name to import the other. The new name also says where the plan is headed: adaptive execution (M5) re-plans it with results computed while the query runs, which a compile-time "logical plan" is not.
