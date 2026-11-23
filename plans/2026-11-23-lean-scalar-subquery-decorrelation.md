# Lean model of correlated scalar subquery decorrelation

## Goals

- Prove that `Planner.decorrelateScalar` (aggregation by the correlation keys, left-joined to the
  outer input, with the empty-group value for unmatched rows) gives every outer row SQL's
  subquery value, NULL correlation keys, duplicate outer rows, and groups with no rows included.
- Replay the model against the engine and DuckDB so the proof stays tied to the shipped code
  (M5 follow-up recorded under PR 12).

## Background

- SQL evaluates `(select agg from r where r.k = o.k and locals)` per outer row: the aggregate over
  the rows whose key equality is TRUE, so a NULL outer key sees no rows.
- The engine groups the locally filtered body by the inner keys (NULL keys form a group of their
  own), left-joins on the keys (NULL never matches), and, when the value over no rows is not NULL
  (`count` = 0), projects `CASE WHEN first key IS NULL THEN empty ELSE v END`.
- The planner rejects non-equality correlations, so the model takes `outer = inner` key pairs only;
  the local/correlated split is already proved in `Decorrelate/Rule.lean`.

## Design

- Lane: Express — formal model, fixture, and a conformance test; no engine change unless the
  replay finds a bug.
- New `formal/WvletFormal/Scalar/` on `Algebra/Bag.lean`, reusing `Decorrelate`'s keys, `dedup`,
  and `keysMatch`.
  - The aggregate is an arbitrary function from a bag to a value, so `count`, `sum`, `max`, and
    expressions over them (`case when count(*) > 0 ...`) are all instances.
  - Soundness: the plan keeps every outer row once, in order, with SQL's value.
  - Skipping the CASE is sound exactly when the value over no rows is NULL (the planner's `None`).
- Counterexamples: no empty-group fix with `count`, an inner join, detecting unmatched rows by a
  NULL value (`coalesce`) instead of the key, and NULL-safe key matching.
- `lake exe scalar-subquery-traces` exports outer/inner inline tables with NULL and duplicate keys
  and a set of subqueries (count(*), count(col), sum, max, a CASE over count, a local filter, two
  keys) with the model's value per outer row.
- `ScalarSubqueryConformanceTest` (engineJVM) reads each outer row's value back through WHERE
  conditions (`is null`, `= v`, and one under OR; the engine binds correlated scalar subqueries in
  conditions only), compares rows with the model and DuckDB, and checks the plan: one left join
  over an aggregation, with the `CASE WHEN key IS NULL` exactly when the value over no rows is not
  NULL.
- Formal workflow freshness step, CLAUDE.md formal section, and formal/README.md updated.

## Alternatives and Why Not?

- Model via `Algebra.leftJoin` over merged rows: the output rows mix group columns into the outer
  row, which makes the per-row statement harder; the `markJoin` style (outer row plus value) states
  the result directly, as PR 12 did.
