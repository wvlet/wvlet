# Lean model of EXISTS / IN decorrelation (optimizer M5)

## Goals

- Prove that the engine's decorrelated plans of correlated `[NOT] EXISTS` and `IN` keep SQL's rows under three-valued logic, NULLs and duplicates included.
- Replay the model against the engine over inline tables with NULLs, as the earlier models do.
- Replay the pushdown model's semi and anti join cases (proved in PR 9, never replayed, since only EXISTS / IN reach those joins).

## Background

- `Planner.correlatedJoin` turns a correlated EXISTS / NOT EXISTS / IN filter conjunct into a semi or anti join: `planCorrelated` keeps the subquery's conjuncts that read only its own input as a filter on the body, the correlation equalities become hash keys, the other correlated conjuncts a residual; IN also joins on `target = value`.
- `Planner.decorrelateExists` binds an EXISTS under OR / NOT as a mark: a left join with the distinct correlation keys of the body and `key is not null`. Only equality correlations are supported.
- NOT IN (null-aware anti join) already has its own model (`NotIn/`); `decorrelateScalar` (correlated scalar subqueries, the count-on-empty value) is left to a follow-up to keep this PR small.

## Design

- Lane: Express — a formal model, a fixture, and a conformance test; no engine change unless the replay finds a bug.
- `formal/WvletFormal/Decorrelate/Rule.lean`, on `Algebra/Bag.lean`:
  - SQL's meaning: EXISTS is "some inner row makes the subquery's WHERE TRUE" with the outer row in scope; IN is `inTri` over the values of the matching inner rows.
  - The plans: conjunct split (local filter on the body, correlated conditions on the join), the semi / anti join, the IN semi join, and the mark join over deduplicated keys.
  - Theorems: each plan returns SQL's rows; the mark is SQL's EXISTS value (never unknown) with exactly one output row per outer row, so any condition over it under OR / NOT keeps SQL's rows.
- `Decorrelate/Necessity.lean`: counterexamples for unsound variants — EXISTS as an inner join (duplicates outer rows), inner join plus DISTINCT (loses duplicate outer rows), the mark join without DISTINCT (duplicates), a semi join for EXISTS under OR (drops rows), and NOT EXISTS rewritten as NOT IN (unknown on NULL keys).
- `lake exe decorrelate-traces` writes `decorrelate-traces.json` (engineJVM resources): EXISTS / NOT EXISTS / IN conjuncts over several subquery WHEREs (key only, key plus residual, key plus local filter, no key, two keys), EXISTS under OR / NOT, and the pushdown model's semi / anti cases (placement and rows).
- `DecorrelateConformanceTest` (engineJVM): rows against the model and DuckDB, the join kinds of the plan, and for the pushdown cases where `PushDownFilters` puts the conjunct.
- Formal workflow freshness step, CLAUDE.md formal section, and formal/README.md (layout, sync, known gaps) updated.

## Alternatives and Why Not?

- Extend the pushdown fixture with semi / anti cases instead: the queries need EXISTS subqueries, which belong with the decorrelation fixture; the cases still come from `Pushdown.place` / `rewrite`.
