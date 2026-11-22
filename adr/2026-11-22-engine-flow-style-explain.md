# ADR: Flow-style EXPLAIN for engine physical plans

Date: 2026-11-22 · PR #TBD

## Context

`Operator.explain` printed an indented tree with the root on top, every child one level deeper than its parent. The golden plans under `spec/engine-plans/` (PR #2256) use this text, and the optimizer milestones will change these plans often. In an indented tree, adding one operator (e.g. a filter that pushdown moves) re-indents the whole subtree below it, so a one-line plan change shows up as dozens of changed lines in a golden diff. The compiler's `LogicalPlanPrinter` already prints logical plans in flow style, in the order data moves, as Wvlet queries are written.

## Decision

- **Data flows down.** A plan prints from its sources to its root: the first child continues the flow, and the operator follows on the next line at the same indentation, marked `↓ op`.
- **Other inputs are nested above their reader.** The other children (hash-join build sides, set-operation inputs) print one level deeper, just above the operator that reads them, and that operator is marked `↙ op`, as in `LogicalPlanPrinter`. Inputs that are not children print the same way: the body of a shared CTE under its first reader (headed `(shared)`), and the precomputed right side of a parallel cross join (`CrossInput`). Their operators override `extraInputs`.
- **Leaves have no marker.** A scan or a values list starts a flow.
- **Tests read the flow, not the indentation.** `PlanText.downstream(plan, op)` lists the operators an operator's output flows into, which replaces regexes over parent and child lines.

## Consequences

- An operator added to or removed from a plan changes one line of its golden file. Moving a join input changes only that input's lines.
- Leaves keep the same order as before (the first child before the others), so tests that read the scan order of a plan still hold.
- The physical and logical plans read the same way, which the planned logical IR's text form (optimizer milestone M2) can follow.
