# Engine: constant folding and cast unwrapping

## Goals

- Fold expressions over literals (`1200 + 11`, `cast(1 + 10 as decimal(21,2))`, `1 < 2`) at bind time, so the bounds reach scan filters and row-group pruning and aren't recomputed per batch.
- Compare a column directly when a comparison casts it losslessly to a wider type (`cast(i_current_price as decimal(12,2)) > 50.00` → `i_current_price > 50.00`), so no per-row cast runs and integer bounds become scan filters.
- Targets: TPC-DS q87, q73, q41, q97, q02 (`d_month_seq <= 1200 + 11` stays out of the date_dim scan filter), q80, TPC-H q19.
- PR 1 of 2. Per-table filters implied by an OR of ANDs (q19 part scan) is the follow-up PR, with the FactorOr Lean model extended first.

## Background

- The binder folds only casts of a literal and calls of row functions whose arguments are all literals (`plan/Binder.scala` `bindFunction`). `Arith`, `Negate` of non-literals, `Compare`, `CaseWhen`, `Coalesce` and boolean connectives over literals stay as expressions.
- `ScanFilters.toParquet` translates only `column op literal`, so `d_month_seq <= lit(1200)+lit(11)` is dropped from the scan filter while `>= 1200` is kept.
- The binder casts both sides of a comparison to their common type, so a narrower column is wrapped in a `CastExpr` evaluated for every row.
- Neither rewrite touches the areas that have Lean models (pushdown, NOT IN, join-key filters, FactorOr, decorrelation).

## Design

- Lane: Express — a pure expression rewrite inside the engine behind toggles, no user-facing change, no always-gate area; the direction comes from the approved work item.
- Two `Optimization` toggles so each can be A/B'd: `fold_constants` and `unwrap_casts`.
- Folding happens in the binder, where every expression is built, so filters, projections, join conditions and the planner's own paths all see folded expressions; no separate optimizer pass.
- A subtree folds when it doesn't read any column and has no volatile function (`random`, `uuid`): it is evaluated on a one-row batch and replaced by a literal of the same type. If the evaluation throws (division by zero, bad cast), the expression stays as is, so an error still occurs only when rows are evaluated.
- `and`/`or` with a literal `true`/`false` operand simplify the way three-valued logic allows (`false and x` → `false`, `true and x` → `x`, and the duals).
- Cast unwrapping applies to `cast(col as T) op literal` and `cast(col as T) [not] in (literals)` when the cast is lossless and order-preserving (integer widening, integer to a decimal wide enough, decimal to a decimal with no fewer integer and fraction digits) and every literal converts exactly to the column type. Otherwise the cast stays.
- Range-pair simplification (`x > 5 and x > 7`) is left for later; nothing in the targets needs it.
- Tests cover folding of each expression kind, the error and volatile cases, each unwrap rule with exact and inexact literals, and the scan filter for `between 1200 and 1200 + 11`; golden plans are regenerated.

## Alternatives and Why Not?

- A separate optimizer pass over the runtime plan: it would miss the binder's own uses (planner-side filters, inner-join WHERE folded by the binder) and needs to rewrite every node kind's expressions.
- Unwrap by rounding the literal (`int_col < 5.5` → `int_col <= 5`): more cases, more 3VL/rounding tests, no target query needs it.
