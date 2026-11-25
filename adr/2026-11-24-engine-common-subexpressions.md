# Common subexpressions are shared when lowering, not by a logical pass

- Date: 2026-11-24
- Status: Accepted

## Context

- TPC-H q1 evaluated `l_extendedprice * (1 - l_discount)` twice per batch, widened and rescaled the literal `1` per batch, and kept duplicate `sum` partials for each `avg`; decimal arithmetic on aggregate arguments was 18% of q1's CPU at SF10.
- DuckDB (`CommonSubExpressionOptimizer`) and DataFusion (`CommonSubexprEliminate`) share repeated expressions as logical rewrites that add a projection with new columns.
- The engine's logical passes over `RuntimePlan` cannot allocate fresh `ColumnId`s (only `RuntimeBinder` does), and a new Project between nodes changes what `Stats`, `PushDownFilters`, and the Lean-modelled rules see.

## Decision

- `Lowering` shares repeated subexpressions over input positions (`common_subexpressions`): for a Project, Filter, Aggregate, or Window, it appends the shared expressions in a `ProjectOp` below and rewrites the operator's expressions to read them. The largest repeat goes first; nested repeats go to an earlier projection.
- An expression is shared only when at least one occurrence is evaluated for every row, not only in a later CASE branch or COALESCE argument, so sharing never evaluates what the operator would skip. Volatile functions are never shared, and expressions match with `==`, not `VExpr.sig`.
- `Lowering` also folds row-independent subexpressions and turns integral constants in decimal `+` / `-` into decimals at the result scale (`fold_constants`). Two-phase aggregation merges identical partial aggregates (`shared_agg_partials`), so `avg(x)` reuses `sum(x)` and `count(x)`.
- A window under a rank bound (`window_top_n`) is lowered without sharing, so the bound still reaches its `WindowOp`.

## Consequences

- With all three on, TPC-H SF10 q1 ran in 0.82x and the TPC-H SF10 suite in 0.983x; the SF1 suites didn't change.
- The physical plans of spec/tpch and spec/tpcds show the shared projections (`__cse_<n>` columns), and golden plans changed with them.
- Sort keys aren't shared, and `count(x)` isn't rewritten to `count_star()`, since column types don't carry nullability.
