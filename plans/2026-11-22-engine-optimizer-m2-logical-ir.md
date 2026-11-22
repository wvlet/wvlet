# Engine optimizer M2: logical IR, binder, and lowering

Milestone M2 of `plans/2026-11-22-engine-optimizer-framework.md` (sections 1, 2, 6, 7).

## Goals

- Give `wvlet-engine` a logical stage between the compiler's `Relation` and the physical `Operator`s, so that M3 can move rewrite rules onto it one at a time.
- Columns get identities (`ColumnId`) and expressions are bound once, so later rules compare ids, not names or SQL text.
- No behavior change in this milestone: the logical path lowers to the same physical plans as today's `Planner`.

## Background

- `Planner.planRelation` binds names, optimizes, and lowers in one recursion over about 50 relation kinds. A full rewrite in one PR would collide with the sessions editing `Planner` now.
- Golden physical plans for TPC-H and TPC-DS (M0) can prove that a second planning path produces identical plans.

## Design

Lane: Express. Internal refactor behind a switch whose default keeps today's planner; golden plans prove the new path is identical.

- `wvlet.lang.engine.logical.LogicalPlan`: a closed `enum`. PR 1 has `Scan`, `Values`, `Filter`, `Project`, `Sort` (with an optional TopN limit), `Limit`, and `Legacy`. PR 2 adds `Join` and `Aggregate`.
- Every output column is a `Column(id, name, type, qualifiers)`. Expressions are `BExpr`: today's executable `VExpr` whose column references hold `ColumnId`s instead of positions. Lowering maps them back to positions. Reusing `VExpr` avoids a second expression language with ~1.4k lines of function binding.
- `LogicalBinder` turns a `Relation` into a `LogicalPlan` for the core forms and the wrappers that only rename or requalify columns. Anything else becomes `Legacy(op)`: the old `Planner` plans the subtree at bind time, in the same order as today, so its side effects (CTE sharing, hidden column names, scalar prefetch) stay identical.
- It routes by the same tests the planner uses. For example, a `Filter` over a join or `GROUP BY`, a filter with a subquery, or a `Project` with aggregates, windows, or `unnest` stays `Legacy`. A failed bind is an error, as it is today; it never falls back after binding a child.
- `Lowering` maps ids to positions and builds the same operators as `Planner`. The physical passes (`ColumnPruner`, `Parallelizer`) are unchanged.
- `PlanValidator` checks that every referenced id is produced by the child, types agree, and output ids are unique. It runs when `OptimizerOptions.validatePlans` is on (the logical-path tests).
- `wvlet.engine.planner=logical|legacy` (or `WVLET_ENGINE_PLANNER`) picks the path, default `legacy`. The option lives on `OptimizerOptions`, so tests can choose it per engine.
- Non-exhaustive match warnings become errors for `wvlet-engine`, so a new `LogicalPlan` case that a pass doesn't handle fails the build.
- Tests: per-node binder/lowering tests with a stable `name#id` text form; the golden plan test also runs with the logical planner and must match the same files; spec/basic runs on the logical path.

Split:

- PR 1 (this one): IR, binder for the nodes above, lowering, validator, switch.
- PR 2: `Join` (inner-join trees through `JoinOrderOptimizer`, outer joins) and `Aggregate`, then switch the default when TPC-H/TPC-DS plans and benchmarks match.

## Alternatives and Why Not?

- A new `BExpr` tree mirroring every `VExpr` kind: duplicates the function binder now, before any rule needs more than ids. M3 can add structure when a rule needs it.
- Bind lazily at lowering time: legacy subtrees would be planned in a different order, which changes hidden names and CTE sharing in the golden plans.
