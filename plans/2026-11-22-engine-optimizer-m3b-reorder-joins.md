# Engine optimizer M3b, PR 1: ReorderJoins on the runtime plan

## Goals

- Choose the order of inner joins in an optimizer pass over the `RuntimePlan`, instead of while binding. Later rules (Normalize of join conditions, PushDownFilters, Decorrelate) can then rewrite joins before their order is chosen.
- Read row and distinct-count estimates from runtime plan nodes (by column id), not from lowered operators.
- Keep the golden plans of both planners unchanged, and keep the benchmarks.

## Background

- Today `RuntimeBinder` calls `Planner.innerJoinTree` with each leaf already optimized and lowered, because `JoinGraph`, `JoinOrderOptimizer`, and `CardinalityEstimator` read operator schemas and estimates. Every level of a nested join tree runs the optimizer again on its subtrees.
- `EnginePlanGoldenTest` checks both planners against the same golden files, so the runtime pass must make the same choices as the legacy planner, which keeps its own copy until M4 deletes it.
- Conditions with subqueries (EXISTS, IN, correlated scalars) become joins built by the legacy planner on lowered operators. Until Decorrelate moves (a later M3b PR), they still apply on top of the ordered tree.

## Design

Lane: Express — an internal planner refactor that keeps plans the same, behind the existing `reorder_joins` and `swap_build_side` toggles.

- The binder turns a tree of inner joins into one unordered node: the same leaves as `flattenJoins`, plus its bound conditions (the ON conditions and the filter conjuncts over it). The node outputs the leaves' columns in written order; parents read them by id, so the chosen order doesn't matter to them.
- This differs from the framework plan (section 2, item 4: the pass "extracts the maximal inner-join graphs" from `Join` trees). An explicit node keeps the leaves the binder chose; see Alternatives. The framework plan is amended to say so.
- A `ReorderJoins` pass, after `Normalize`, replaces each such node from the bottom up. It pushes single-input conditions to their leaves, turns equalities between inputs into hash join keys, and runs today's multi-start greedy search. The output is the same columns in join order. Lowering rejects the unordered node, so the pass always runs; `reorder_joins` off keeps the written order, as today.
- Estimates come from a small `Stats` reader over runtime plan nodes: rows per node, and distinct counts traced by column id to source statistics. It follows the operator estimator rule for rule, and reads a `Legacy` leaf through the operator estimator, so the two planners agree.
- Selectivity keeps today's rule: 1/ndv for `x = literal` and n/ndv for `x in (n literals)`, decided on the query's condition. The binder records the literal count with each bound condition, because a bound constant may also be a folded expression or a scalar subquery result, which today get the default. Estimating from bound constants is a separate PR with its own benchmarks.
- Binding keeps the planner's order: leaves in written order, then each leaf's single-input conditions in input order (these may run scalar subqueries, as pushing them does today), then the conditions between inputs. Only these last ones bind earlier than today (before the search rather than as it joins), and binding them has no planner side effects unless they hold a scalar subquery; the golden plans would show a change.
- A join tree with subquery conditions (EXISTS, IN, correlated scalars) is ordered right away in the binder, by running the pass and lowering, so the legacy planner can apply those conditions over the ordered tree, as today. They stop being legacy nodes with Decorrelate.
- The greedy search becomes generic over the plan and condition forms. The legacy planner keeps calling it over operators and compiler expressions, and the pass over runtime plans and bound expressions. One copy of the search means the two planners can't drift apart before M4.
- Deleted: the runtime binder's call into `Planner.innerJoinTree`, which goes back to building operators only. The legacy copy (`innerJoinTree`, `JoinGraph.build`, the operator estimator) stays until M4 deletes the legacy planner. OR factoring of join conditions stays in `Planner.conjuncts` in this PR and moves into `Normalize` in the next one.
- Tests: per-pass tests with `Optimizer.only` check that the pass fires and orders a small star join. The golden plans, conformance (spec/basic, TPC-H, TPC-DS), and the toggle test check the rest.

## Alternatives and Why Not?

- **Bind a written-order tree of `Join` nodes and have the pass extract maximal inner-join graphs.** Joins inside a parenthesized subquery would merge into the outer graph, which changes plans. An explicit node keeps the leaves the binder chose.
- **Estimate selectivity from bound constants now.** TPC-DS has `d_month_seq = (select ...)` conditions whose estimate would change from 0.3 to 1/ndv in both planners. That's a plan change that needs its own benchmarks.
- **Keep join trees with subquery conditions on `innerJoinTree`.** It would keep two binder paths for the same tree until Decorrelate; running the pass early costs nothing extra.
- **Keep two copies of the greedy search.** Small differences would show up as golden drift between the planners.
