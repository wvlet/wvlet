# Engine: common subexpressions, shared aggregate partials, and pre-scaled constants

## Goals
- Cut the decimal arithmetic on aggregate arguments (18% of TPC-H q1 CPU at SF10) and the duplicate accumulators, without changing results.
- Make repeated expressions in select items, filters, aggregate arguments, and window arguments evaluate once per batch, as DuckDB's `CommonSubExpressionOptimizer` and DataFusion's `CommonSubexprEliminate` do.

## Background
- Operators evaluate each expression on its own: q1's `l_extendedprice * (1 - l_discount)` runs twice per batch (once alone, once inside `* (1 + l_tax)`).
- `avg` is already split into `sum` + `count` by the two-phase aggregation (`TwoPhaseAgg.decompose`), but the partials aren't deduplicated: q1 runs `sum(l_quantity)` and `sum(l_extendedprice)` twice each (11 partial accumulators where 9 suffice).
- `1 - l_discount` evaluates `lit(1:int)` as an int vector, widens it to longs, then scales it by 100 on every batch (`asLongs` + `scaleIntegral`) before subtracting.
- Engine expressions don't fold constants either: q19 evaluates `cast((1 + 10) as decimal(21,2))` per batch.
- None of this touches the Lean-modelled rules (`RewriteExpr`, name lookup, `PushDownFilters`, NOT IN, `FactorOr`, decorrelation); the rewrites run after the logical optimizer.

## Design
- Lane: Standard (pre-approved via the work item); planner/lowering and expression evaluation only, no change to the aggregation operators the wide-agg session is editing.
- Three toggles, all on by default, each A/B-able with `WVLET_ENGINE_BENCH_AB`:
  - `common_subexpressions`: when lowering a Project, Filter, Aggregate, or Window, repeated non-trivial subexpressions across its expressions are computed once in a Project below and referenced by position. The largest repeated subtree wins (q1: `#1*(1-#2)` is computed once; `sum(t)` and `sum(t*(1+#3))` read it).
  - `shared_agg_partials`: two-phase aggregation merges identical partial aggregates, so `avg(x)` reuses an existing `sum(x)` (and `count(x)`).
  - `fold_constants`: constant subtrees are evaluated once at plan time, and an integral constant in decimal `+`/`-` becomes a decimal constant at the result scale, so the per-batch widen-and-scale disappears.
- Safety rules for hoisting:
  - A subexpression is hoisted only if at least one occurrence is evaluated unconditionally (not only in a later CASE branch or COALESCE argument), so hoisting never evaluates something that would otherwise be skipped.
  - Volatile functions (`random`, `uuid`) are never merged; expressions are matched with `==`, not `VExpr.sig` (sig ignores RowFn closures).
  - A filter keeps its original output columns (via `FilterOp`'s `keep`).
  - Folding that throws at plan time leaves the expression unfolded, so errors still surface only when rows exist.
- Not doing `count(x)` → `count_star()`: column types don't carry nullability, and every TPC-DS column is nullable.
- Golden plans change (extra Projects, fewer partials); regenerate them. Tests cover each rewrite and results with each toggle off (`EngineOptimizerToggleTest`).
- Measure: SF10 (10 runs) and SF1 with `WVLET_ENGINE_BENCH_AB=<toggle>` per toggle, reading engine ms.

## Alternatives and Why Not?
- A logical `RuntimePlan` pass: needs fresh `ColumnId`s, which passes don't allocate today, and new Projects would shift estimates and pushdown; lowering works on positions and runs after those decisions.
- Caching subexpression results inside `VExpr` per batch: expressions are shared across morsel threads, so a cache would need synchronization.
