# Short-circuit AND/OR over surviving rows in FilterOp

## Goals

- Stop evaluating later conjuncts of a filter on rows an earlier conjunct already rejected (and, for OR, later disjuncts on rows already accepted), as DuckDB's `ExpressionExecutor::Select` and DataFusion's `PreSelection` do.
- Run cheap conjuncts (primitive comparisons) before expensive ones (string LIKE, IN lists, row functions), as DuckDB REORDER_FILTER and DataFusion `reorder_predicates` do.
- Targets: TPC-H q19, q6, q12; TPC-DS q13, q48, q28, q88.

## Background

- `AndExpr` / `OrExpr` evaluate both sides over the full batch and combine masks (`expr/VExpr.scala:222`); `FilterOp` evaluates its condition once per batch (`exec/Operators.scala:162`).
- Batches can defer their columns (`Batch.take` with row positions, `late_gather`), so evaluating an expression over a subset of rows costs a gather of only the columns it reads.
- Another session is changing Normalize / FactorOr; this change stays in physical evaluation, so logical plans and golden plans don't change.

## Design

- Lane: Express. Internal engine performance change behind toggles, no user-visible behaviour change, no always-gate area; merge authorized by the engine optimization loop rules.
- `FilterOp` computes the TRUE row positions of its condition by walking AND/OR: an AND narrows the positions conjunct by conjunct and stops when none remain; an OR evaluates each disjunct on the rows not yet accepted and merges the positions. Other expressions are evaluated as before, on the subset.
- Gathering a subset costs about as much as a cheap comparison, so a conjunct runs on the full batch (masked with the current positions) when most rows still survive; cheaper conjuncts need a smaller surviving share to switch to the subset.
- Cheap-first order: conjuncts (and disjuncts) are stably sorted by a static cost. Conjuncts that can throw (non-try casts, division, row functions) are barriers: nothing moves across them, so a throwing conjunct never sees more rows than in the written order.
- Only TRUE positions matter for a filter, so three-valued logic is preserved: AND is TRUE iff both are, OR iff either is.
- Two toggles: `short_circuit_filters` (narrowing evaluation) and `reorder_conjuncts` (cheap-first order, applied only with narrowing). Plan text is unchanged.
- Tests cover AND/OR/NULL combinations against the full-batch evaluation, deferred input batches, empty survivors, and the barrier rule.

## Alternatives and Why Not?

- Rewrite `AndExpr.eval` itself to short-circuit: eval must return a full-size boolean vector (projections, CASE, join residuals use it), so the subset results would need scattering back; selection only pays off where the consumer wants positions, i.e. FilterOp.
- Adaptive reordering by measured cost (DuckDB AdaptiveFilter): left as a follow-up once the static order is measured.
