# M5: Lean model of NOT IN as a null-aware anti join

## Goals

- Prove that the engine's binding of `x NOT IN (subquery)` returns SQL's rows under three-valued logic, NULLs on either side and an empty subquery included.
- Show with counterexamples why a plain `NOT EXISTS` anti join on `x = y` would be wrong.
- Replay the model's cases against the engine through a `lake exe` fixture, as the filter pushdown model does.
- Fix the bugs the model finds, minimally.

## Background

- SQL: `x IN S` is the Kleene OR of `x = y` over the values `y` of `S`, and FALSE for an empty `S`. So `x NOT IN S` is TRUE when every `x = y` is FALSE, unknown when none is TRUE but some is unknown, and TRUE for an empty `S`, even when `x` is NULL.
- The engine binds an uncorrelated NOT IN conjunct (`RuntimeBinder.plans.subqueryJoin`) like this: no rows when the subquery has a NULL, otherwise a hash anti join on `x = y` followed by `x is not null`.
- A correlated NOT IN (`Planner.correlatedJoin`) becomes an anti join whose residual is `x = y or x is null or y is null`.
- Elsewhere (a select item, under OR or NOT, or with `Decorrelate` off), the binder evaluates the subquery at plan time into an `InList` value set.
- A probe on origin/main found three bugs:
  - `where a not in (<empty>)` drops the rows where `a` is NULL. The `is not null` filter also runs when the subquery is empty.
  - `select a not in (from ...)` binds the subquery as a scalar subquery. It fails with "more than one row", and gives NULL when the subquery is empty. Both parsers emit `In` / `NotIn` with a one-item list. `Binder.expandTemplates` turns that into `InRelation` for filter conjuncts only, so a select item (Wvlet or SQL) reaches `Binder.bindIn`.
  - `InList` returns unknown for a NULL target even when the value set is empty. It should return FALSE for IN and TRUE for NOT IN.

## Design

- Lane: Express. It fixes a clear SQL NULL bug, adds no new optimization or toggle, and touches no always-gate area. The launch prompt authorizes merge.
- Lean core (`Algebra/Bag.lean`):
  - `inTri v vs`: Kleene OR of `eqTri v y`, FALSE for an empty list.
  - `notIn`: the rows of the left input where `not (inTri ...)` is TRUE (the SQL reference).
  - `nullAwareAntiJoin`: keeps a left row when every right row compares FALSE.
- New `formal/WvletFormal/NotIn/` model:
  - `Rule.lean` holds the engine's three bindings:
    - uncorrelated: empty subquery, then every row; a NULL in the subquery, then none; otherwise an anti join on `x = y` plus `x is not null`;
    - correlated: an anti join on `corr and (x = y or x is null or y is null)`;
    - elsewhere: the `InList` value set, proved equal to `inTri`.
  - The proofs show that the null-aware anti join and both bindings equal `notIn`. The correlated binding is proved against `notIn` over each row's correlated matches.
  - `Necessity.lean` gives counterexamples:
    - a plain anti join keeps a NULL target;
    - it keeps rows when the subquery has a NULL;
    - the old binding (no empty check) drops a NULL target over an empty subquery;
    - the old `InList` makes a NULL target unknown over an empty set.
- Engine fixes (three small edits; no plan changes for a non-empty subquery). Golden TPC-H q16 loses its anti join, because its `s_comment like '%Customer%Complaints%'` subquery is empty in `spec/tpch/data`. It has 1 row at SF0.1 and 4 at SF1, so the benchmark plans are unchanged:
  - `subqueryJoin` returns its input when the subquery is empty.
  - `bindIn` binds a single subquery item through `Planner.bindInSubquery`, which builds a value set.
  - `InList` returns `negate` for a NULL target when the value set is empty and has no NULL.
- Fixture (`lake exe not-in-traces`):
  - The left table has NULL and duplicate targets. It meets six subqueries: values without a NULL, values with a NULL, only NULL, empty, duplicates, and no overlap.
  - Each case gives the WHERE rows and the per-row value of `a in` / `a not in` in a select item (TRUE, FALSE, or NULL).
  - A NOT IN correlated by `k = b` covers the null-aware residual join, an empty group included.
- `NotInConformanceTest` (engineJVM):
  - It replays every case with the default optimizer and with `Decorrelate` off, so both the anti join path and the value-set path run. The select items run in both Wvlet and SQL, and DuckDB must return the same rows.
  - It checks the bound plan's shape: no join over a subquery with a NULL, no join when the subquery is empty, and an anti join otherwise.
- Docs: a formal/README.md section with its known gaps, the CLAUDE.md formal bullet, and the Formal workflow freshness step.
- Gaps recorded on the item and in the README: tuple NOT IN (`bindTupleInSubquery`), and the casts in `inKeys`.

## Alternatives and Why Not?

- Model only and leave the bugs for a later PR: the fixture would have to encode wrong rows, and the fixes are three lines.
- Replace the uncorrelated binding with the correlated null-aware residual join: it is the general form, but it changes plans and needs benchmark evidence. A two-line empty check keeps today's fast anti join.
