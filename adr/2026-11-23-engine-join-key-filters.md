# ADR: Filters carry across hash join keys

Date: 2026-11-23 · PR (M5, join-key filters)

## Context

At SF1, TPC-DS q78 ran 11× slower than DuckDB (444 ms vs 40 ms), the largest absolute gap in the suite. It filters `ss_sold_year = 2000` above `ss LEFT JOIN ws ON ws_sold_year = ss_sold_year ...` (and the same for `cs`), where each CTE aggregates `sales × date_dim` by `d_year`. `PushDownFilters` pushed a conjunct only into the preserved input of an outer join. So `ws` and `cs` aggregated every year before the joins, and their large builds were copied on probe. It also stopped at a `Join[Inner]`: the binder orders an aggregation's input while binding it, so only an `InnerJoins` graph (not yet ordered) took conditions. The `ss` filter therefore stayed above `store_sales × date_dim`, and the runtime join filter on `ss_sold_date_sk` got every `date_dim` key.

## Decision

- With `join_key_filters`, `PushDownFilters` carries a conjunct across the keys of a hash join. When a conjunct moves into the preserved input of a LEFT or RIGHT join, or into the left input of a semi or anti join, and reads only that input's key columns, it is also pushed into the other input, with each key column replaced by its partner key.
- The other input's rows that fail the copy (FALSE or NULL) can only match preserved rows that the conjunct drops. A match needs equal, non-NULL keys, so on two matching rows the conjunct has the same value. This holds for any deterministic conjunct over the keys (`= 2000`, `IN`, ranges, `IS NULL`, `coalesce(k, 0) = 0`), not only `= constant`, and whatever the residual is.
- A conjunct over the other (NULL-supplying) input of an outer join is never carried. It reads NULL on padded rows, so filtering the preserved input by it would drop rows (`Findings.lean`).
- Inner joins the binder already ordered take conjuncts into the input they read, and across their keys both ways. The pass does not turn them back into `InnerJoins`: that would re-order joins the binder ordered with its own estimates.
- Only key pairs of plain columns of the same type carry conjuncts. Floating-point keys are left out, because hash key equality and `=` disagree there (`-0.0` and `0.0`, NaN). `key IS NOT NULL` isn't carried either: a hash join already drops NULL keys on both sides.
- A conjunct a filter already applies isn't added again (`filter` drops repeated signatures), since a carried conjunct can meet the same one pushed from below on a second optimizer run.
- The Lean model (`formal/WvletFormal/JoinKeyFilter/`) proves the rule over bags with three-valued conjuncts for inner, LEFT, RIGHT, semi, and anti joins. `JoinKeyFilterConformanceTest` replays its traces through the engine with the toggle on and off, and against DuckDB.

## Consequences

- q78's three `date_dim` scans read only `d_year = 2000`, and the `ss_sold_year` filter above the joins disappears. Golden plans q04, q11, q31, q39, q64, q74, q75, q78 (TPC-DS) and q16 (TPC-H) change, the same set at SF1. Most move a year or month filter from above an ordered join onto `date_dim`.
- When a filter leaves the top of a join, that join no longer fuses the column pruning that the filter's `keep=` did, so the operators above it pass a few more columns (q39, q16).
- FULL joins carry nothing, as before: no conjunct passes through them.
