# Engine optimizer M5, PR 2: re-plan joins the binder ordered over shared CTEs

## Goals

- Extend PR 1's re-plan (ADR 2026-11-23-engine-replan-materialized-ctes) to joins the binder orders while binding: aggregation, pivot, and subquery inputs lowered by `RuntimeBinder.lowered`. When such an input joins a shared CTE, its join order and build sides use the CTE's exact row count instead of 1e6.
- Keep the PR small: the bound inputs of the root plan only. CTE bodies that join other shared CTEs and per-reader stats stay follow-ups.

## Background

- `RuntimeBinder.lowered(p)` optimizes an input before its parent is built, because the shared planner helpers read schemas and estimates from operators. The parent embeds the optimized input `o`, whose `InnerJoins` are already ordered `Join` trees. PR 1 re-runs the passes over the root's bound plan, but `ReorderJoins` only orders `InnerJoins`, so `o` keeps its 1e6 order.

## Design

- The binder records each input it optimized: optimized plan → the bound plan it came from (by identity, only when they differ). Entries are cleared when the root plan is done.
- When PR 1's step looks for CTEs read below joins, it follows a recorded input into its bound form, so `InnerJoins` inside an aggregation input count too.
- After the CTEs are computed, the root's bound plan is rebuilt bottom-up: a recorded input that reads a computed CTE is optimized again from its bound form (its own recorded inputs first). Then the root passes run as in PR 1, and the plan is lowered again only if it changed.
- `ReorderJoins` outputs columns in join order, so a re-ordered input may output the same columns in a different order. Parents read columns by id, but a few read them by position (union, set operations, the root output), so the new input is wrapped in a `Project` that restores the first order. If the column ids differ, the first input is kept.
- The binder's other decisions over the input (the build side of a join it builds itself, e.g. a correlated or semi join) keep their first choice; the root `ReorderJoins` still re-decides outer hash joins.
- Toggle `replan_bound_cte_joins` (on by default; only acts with `replan_materialized_ctes`) for A/B benchmarking on one build.
- Tests: an aggregation over a join of a large table with a small shared CTE flips its build side; toggle off gives PR 1's plan; conformance over spec/basic, TPC-H, TPC-DS.

## Alternatives and Why Not?

- Keep bound inputs unordered (`InnerJoins`) in the parent: the helpers need operators for schemas and estimates, and the first root run would then optimize inputs in the parent's context, changing plans that don't read a CTE.
- Flatten ordered `Join` trees back into `InnerJoins` at re-plan time: the tree has lost the original conditions' grouping (pushed filters, residuals), and outer and semi joins mixed in make the flattening partial.
