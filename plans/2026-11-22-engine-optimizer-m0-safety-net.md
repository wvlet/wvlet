# Engine optimizer M0: golden plans and disable flags

## Goals

- Give the engine planner a safety net before the optimizer framework refactor (M1+, see `plans/2026-11-22-engine-optimizer-framework.md` section 7): plan changes show up in diffs, and each optimization can be turned off on its own.
- No behavior change: with no flag set, the planner produces the same plans and results as today.

## Background

- Today's optimizations are inlined in `Planner` (OR factoring, greedy join order, build-side swap, scalar-subquery prefetch, shared CTEs) and in the physical passes `ColumnPruner` (also row-group filters) and `Parallelizer`.
- Several sessions edit `Planner` concurrently, so the diff must stay small: one flag check per spot, no restructuring.
- `explain` output names the parallelism (`Morsel[parallelism=N]`), which depends on the machine's cores.

## Design

Lane: Express — tests plus an off-by-default flag, no behavior change, no always-gate area.

- Names (stable; later milestones reuse them as pass and rule names, snake_case as DuckDB's `disabled_optimizers`): `factor_or`, `reorder_joins`, `swap_build_side`, `prefetch_scalar_subqueries`, `share_ctes`, `prune_columns`, `push_scan_filters`, `parallelize`.
- `wvlet.engine.optimizer.disabled=<names>` as a system property (or `WVLET_ENGINE_OPTIMIZER_DISABLED`, like the parallelism setting), or `OptimizerOptions` passed to `WvletEngine`. An unknown name is an error, so a typo doesn't silently test nothing.
- `reorder_joins` off joins the inputs in written order, taking the first input connected by an equality (else the first remaining), so comma joins don't become cross products.
- `prune_columns` and `push_scan_filters` are independent: with pruning off, the pruner still walks the plan and attaches row-group filters but keeps every column.
- Golden plans: `spec/engine-plans/{tpch,tpcds}/<query>.plan`, from spec/tpch (checked-in data) and spec/tpcds (the sf=0.1 Parquet that EngineTPCDSTest generates). Machine-dependent parallelism numbers are normalized to `parallelism=N`; the data directory path is replaced by a placeholder. A freshness test fails on drift and prints the regenerate command.
- Differential test: spec/basic and TPC-H run with each name disabled in turn; results must match DuckDB as in the conformance tests.

## Alternatives and Why Not?

- **Parallelism 1 for the golden run.** Hides the Parallelizer rewrites from the golden files, the pass most likely to change.
- **Golden files for spec/sql/tpc-ds too.** Doubles the files for mostly the same plans; can be added later.
