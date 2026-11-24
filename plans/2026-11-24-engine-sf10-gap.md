# Engine: close the gap to DuckDB at SF10

## Goals

- Bring the engine closer to DuckDB at SF10 (baseline main 3a2984f6: TPC-DS 2.36x, TPC-H 3.78x), one focused PR per gap, each measured at SF10 with an SF1 regression check.
- First PR: TPC-DS q95, the largest single gap (2.2–3.3 s vs DuckDB's 54–75 ms, 41–51x, about 11% of the TPC-DS SF10 total).

## Background

- SF10 baseline (main 3a2984f6, `attachments/wvlet-engine-bench/history-sf10/3a2984f6-*.tsv`), largest gaps by engine minus DuckDB time (ms, busy cores engine vs DuckDB):

| suite | query | DuckDB | engine | ratio | cores |
|---|---|---:|---:|---:|---|
| TPC-DS | q95 | 54 | 2,231 | 41x | 11.0 vs 10.4 |
| TPC-DS | q22 | 158 | 888 | 5.6x | 12.4 vs 13.6 |
| TPC-DS | q67 | 599 | 1,306 | 2.2x | 8.4 vs 13.0 |
| TPC-DS | q75 / q51 / q78 | 174–333 | 745–833 | 2.4–4.3x | 5.8–9.6 vs 7.9–11.2 |
| TPC-H | q21 | 221 | 1,445 | 6.5x | 3.7 vs 12.8 |
| TPC-H | q17 / q18 | 140–168 | 771–888 | 4.6–6.4x | 8.1–11.1 vs 10.7–12.5 |
| TPC-H | q7 / q10 / q4 | 63–116 | 470–706 | 6.1–7.5x | 3.7–8.5 vs 10.0–13.6 |

- Core use is the TPC-H story (q21 and q4 keep under 4 of 18 cores busy); q95 is not a parallelism problem.

- q95 is not a join-order problem first. async-profiler at SF10 (this session, 3 runs: engine 3.3 s vs DuckDB 64 ms): 80% of CPU is `HashJoinOp.probeBatch` (pair-array growth, copies, zeroing), and the query allocates 70 GB (1.1 GB at SF1).
- The cost is two join expansions that no output row needs:
  - `ws_wh` (shared CTE): `web_sales ⋈ web_sales` on `ws_order_number` with different warehouses: 74.8M rows at SF10 (6.6M at SF1), 600K distinct orders.
  - `web_returns ⋈ ws_wh` in the second `IN` subquery: 97.9M rows at SF10.
- Both are read only to test membership: `ws_order_number in (...)`. Readers project `ws_order_number` alone, so a semi join gives the same set of values.
- Reduced sizes (DuckDB on the SF10 Parquet): ws_wh as a semi join keeps 7.2M rows (every row of a multi-warehouse order, not 600K), and the second subquery 719K. So 173M expanded rows become about 8M.
- The SF10 plan also semi-joins all 7.2M `web_sales` rows before the dimension joins (the SF1 plan filters to 147 rows first), because the replan reads ws_wh's exact 74.8M rows. With 7.2M rows instead of 74.8M the flip may or may not return to the SF1 order; I'll check the SF10 plan and fix it in this PR only if it still costs time.

## Design

Lane: Standard — a new planner rewrite (behind a toggle) and a multi-PR plan for a broad goal.

- **Semi-join reduction in duplicate-insensitive contexts** (new toggle `reduce_semi_joins`). Where only the set of rows matters, an inner join whose needed columns all come from one side becomes a semi join of that side: X ⋈ Y projected to X's columns has the same distinct rows as X ⋉ Y.
- Duplicate-insensitive contexts: the right (subquery) side of a semi or anti join, the input of `Distinct`, and a group-by with no aggregate calls. The context passes through filters, deterministic projections, sorts, and inner/semi/anti joins.
- Contexts it does not pass through: Window, Limit, Sample, aggregates other than a plain group-by (they count duplicates), and outer joins (kept out for now).
- The rewrite needs at least one equi key between the sides; cross-side non-equi conditions (`wh1 != wh2`) become the semi join's residual.
- Shared CTEs: the body is optimized at its first read, before the kept columns are known, so its rewrite runs at body finalization, after the readers report their columns, the same way reader filters are pushed into it today (ADRs 2026-11-22 shared-CTE pruning and reader filters).
- Each reader reports whether it is in a set context; a reader that reports nothing (an iterator reader, a plan-time subquery) counts as "not". The body is reduced only when every reader is. The decision is made once at finalization, not by catching a later error.
- Lean: a small model on `formal/WvletFormal/Algebra/` proving that the distinct rows of an inner join projected to the left columns equal those of the semi join with the same condition (3VL: a NULL condition drops the pair in both), with a conformance trace test, and a row in design/formal-verification.md.
- Gates: the toggle listed in `OptimizerOptionsTest` and covered by `EngineOptimizerToggleTest`, golden plans regenerated, an ADR for the rewrite, TPC-H 22/22 and TPC-DS 99/99 conformance.
- Tests: rows checked against DuckDB for IN/EXISTS subqueries over joins and shared CTEs, including NOT IN and NULL keys, a reader outside a set context (no rewrite), Window/Limit under the subquery (no rewrite), and the toggle off.
- Rebase on origin/main before each push, since other sessions edit the engine.
- Measure: q95 at SF10 and SF1, plus both SF1 suites with `WVLET_ENGINE_BENCH_AB=reduce_semi_joins`.

## Follow-up PRs (one each, in order; re-profiled at SF10 before each)

- TPC-H multi-way joins over `lineitem` (q21, q17, q7, q10, q4, q18: 4.6–7.5x): profile busy cores and the probe loop at SF10 first.
- TPC-DS wide aggregations and windows (q22, q75, q51, q67, q78).
- Core utilization and scaling (SF1 → SF10 grows 5.8x / 7.5x vs DuckDB's 4.2x / 5.4x).

## Alternatives and Why Not?

- **Fix only q95's join order (CTE estimates).** Leaves the 173M rows of expansion, which is most of the time in both plans.
- **Push the outer order numbers into ws_wh (sideways information passing).** Larger and riskier; the reduction already removes the expansion, and it helps any `IN`/`EXISTS` over a join.
- **Inline ws_wh per reader.** Computes the self-join twice and still expands it.
