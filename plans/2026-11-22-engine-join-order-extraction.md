# Engine optimizer M1: extract join ordering from Planner

## Goals

- Move the inner-join ordering out of `Planner.planJoinTree` into its own units, so the DPccp/DPhyp PR and the later logical-IR move (M3 `ReorderJoins`) change one small file, not the 2.8k-line `Planner` that several sessions edit.
- No behavior change: every golden plan under `spec/engine-plans/` stays byte-identical.

## Background

- `planJoinTree` today does four things in one method: flattens the join tree, pushes single-input conditions to the lowered leaves, precomputes cross-input conditions (`CrossCond`, #2239), and runs the multi-start greedy that builds `HashJoinOp`s directly.
- The estimators (`estimateRows`, `columnDistinct`, `keyDistinct`, `selectivity`, `joinOutputRows`) walk physical operators and are also used by the outer-join and semi-join build-side swap.
- Which inputs a condition reads is still answered by trial binding (`referencesInput`), which needs the Planner's binder; that stays in `Planner` until M2/M3 give columns ids.
- Design: `plans/2026-11-22-engine-optimizer-framework.md` sections 3, 4, 7 (M1 PR 1).

## Design

Lane: **Express** — internal refactor with no behavior change, proven by the golden plans; no always-gate area.

- `CardinalityEstimator` (object, pure over `Operator`s): row and distinct-count estimates, equi-join output rows, and condition selectivity. Selectivity takes a bind function, so it needs no `Planner`. `DefaultSelectivity` moves with it.
- `JoinGraph`: the pushed-down inputs with their estimated rows, and the cross-input conditions as edges with their input sets and equality sides, computed once. Built by `Planner` from the lowered leaves and the per-condition input sets it computes by binding.
- `JoinOrderOptimizer`: today's multi-start greedy, moved unchanged, over a `JoinGraph`. It returns the join tree and the conditions left to apply. It reads `reorder_joins` and `swap_build_side` from `OptimizerOptions` as now.
- Binding stays in `Planner`: the optimizer gets a small callback interface for equi-key binding and filter binding (both need the `Planner`'s `Binder` for scalar subqueries).
- `swappedHashJoin` moves next to the join builder as a pure helper; the outer and semi joins keep calling it.
- `Planner.planJoinTree` keeps flattening, the unnest and outer-join cases, the subquery-condition split, and the final residual filters.
- Tests: golden plans unchanged, `EngineOptimizerToggleTest`, engine conformance (spec/basic, TPC-H, TPC-DS), plus a unit test that the greedy over a small `JoinGraph` joins every input once and places every condition once.

## Alternatives and Why Not?

- Extract and add DPccp in one PR: mixes a move with a plan change, so the golden diff can't prove the move is faithful.
- Move trial binding (`referencesInput`) too: it needs the Planner's binder and is replaced by column ids in M2/M3 anyway.
