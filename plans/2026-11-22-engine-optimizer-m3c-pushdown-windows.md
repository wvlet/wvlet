# Engine optimizer M3c, PR 5: windows on the runtime plan, with filter pushdown below them

## Goals

- Bind a projection that evaluates window functions as a runtime-plan `Window` node under a `Project`, instead of a `Legacy` leaf, so the passes see through it.
- Push filter conjuncts that read only partition keys below a window (framework pass 3, "windows").
- Keep the golden plans unless a change is justified, with benchmark evidence.

## Background

- The legacy planner binds the select items twice: the first pass registers window calls in a `WindowCollector`, then `WindowOp` (input columns, then one `__wv_win<i>` column per call) is built and the items are bound again over it, under a `ProjectOp`. Windows over an aggregation do the same over the aggregation output (after HAVING).
- On the runtime planner a window projection is a `Legacy` leaf (`legacy(other)` in `RuntimeBinder.bind`), and windows over an aggregation turn the whole aggregation into a `Legacy` leaf (`builder.fromOp(ProjectOp(winOp, ...))`). A filter over either stays above it.
- A conjunct that reads only columns every window call partitions by removes whole partitions, so it can run below the window without changing any window value (DuckDB's `FilterPushdown::PushdownWindow` applies the same rule).
- `ColumnPruner` already prunes the inputs of `WindowOp` and keeps every call; `CardinalityEstimator` passes rows through and traces no distinct counts over it.

## Design

Lane: Express. Internal planner change with no user-facing behavior change; the direction and gates were given in the launch prompt.

- **IR node.** `Window(child, calls: IndexedSeq[WindowFunction], columns)`, outputting the child's columns, then one column per call. `WindowFunction` is a `WindowCall` over column ids (arguments, partition keys, order keys). It lowers to `WindowOp` with the same calls and schema, so the operator tree is the legacy one.
- **Binding.** `WindowCollector` works over a schema instead of an operator, and the planner exposes the two-pass binding (`bindWithWindows`), used by both planners. The binder routes a non-aggregating projection with windows (no unnest, no subqueries, as `isPlainProjection` excludes them) to `Project(Window(input))`. In `Planner.aggregation`, windows over the aggregated rows go through a new `AggPlanBuilder.window`, so the aggregation under them stays an IR node too. Column edits with windows (`add`/`prepend`) stay `Legacy`; they move with the other surface forms in M4.
- **PushDownFilters.** A conjunct whose columns are all plain column partition keys of every call moves below the window; the others stay above it. Window results are never read by a pushed conjunct (they're not partition keys).
- **Other passes.** `Stats` passes rows through and traces no distinct counts, as `CardinalityEstimator` does, so join order doesn't change. `PruneColumns` keeps every call and narrows the child to the needed input columns plus the columns the calls read. `PlanValidator` checks the calls' expressions against the child and the column count and types.
- **Golden plans.** Expected to change only where a filter over partition keys sits above a window (or a filter over an aggregation with windows reaches its keys); any change ships with benchmark evidence. Outcome: 8/121 changed (TPC-DS q44, q49, q51, q53, q63, q67, q70, q89), all the same way: a filter over a window result now passes through the projection that names it and sits on the window, as it already does on an aggregation. The operator count is the same. No TPC query filters a partition key above a window. Benchmarks against an origin/main worktree (the legacy planner already differs from main on these queries): 4 alternating rounds of 9 runs on the changed queries came out at 0.98–1.07 (total +1.5%), inside the machine's drift on unchanged-plan queries (0.97–1.17).
- **Tests.** `Optimizer.only(PushDownFilters)` cases: a partition-key conjunct moves below the window and one over a window result or a non-key column stays; windows with different partitions push only shared keys; windows over an aggregation; same rows with the pass disabled. Conformance on both planners, golden plans, and the toggle test.

## Alternatives and Why Not?

- **Push conjuncts over partition-key expressions (`partition by k + 1`).** Correct when the conjunct reads the expression's columns only through it, but no spec query needs it; plain column keys cover TPC-DS and DuckDB's common case.
- **Bind column edits with windows now.** They need the in-place replacement of same-named columns on the IR; that is surface-form work for M4.
- **Keep windows over aggregates as `Legacy`.** Simpler, but a filter over such an aggregation could never reach the aggregation keys on the IR, and M4 has to bind them anyway.
