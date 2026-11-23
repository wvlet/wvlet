# Engine optimizer M4b: retire the `Legacy` leaf

## Goals

- Start the retirement entry of M4b: after this PR the runtime plan has no `Legacy` node, so no runtime-planner query reaches an operator the recursive planner built
- Leave the next PR only the deletion of the legacy planner mode and `Planner.planLegacy`

## Background

- Since #2297 the only `RuntimePlan.Legacy` producer is `RuntimeBinder`'s `legacyConjuncts`, which hands filter conjuncts to `Planner.applyConjuncts` when `subqueryJoins` declines them
- `subqueryJoins` declines in two cases:
  - a toggle is off: `decorrelate_correlated` (correlated [NOT] EXISTS / [NOT] IN conjuncts) or `decorrelate_scalar` (correlated scalar subqueries and EXISTS under OR / NOT / CASE)
  - nothing needs a join: the conjuncts hold only uncorrelated scalar subqueries, and `applyConjuncts` returns a plain filter, which the binder already rebinds as a runtime-plan `Filter`
- The off states of these two toggles have no meaning beyond "plan this with the legacy planner": the legacy planner decorrelates the same way (the same shared `correlatedJoin` / `decorrelateScalar` code), only over operators. The engine has no other strategy for correlated subqueries, such as per-row evaluation
- `decorrelate` keeps a meaning of its own since #2296 (semi joins vs. value-set filters for uncorrelated conjuncts), and its off state never makes a `Legacy` leaf once `decorrelate_scalar` is always on

## Design

Lane: Express. An internal refactor of the engine: results and golden plans stay unchanged; the only visible change is that two internal toggle names are gone.

- Remove the `decorrelate_correlated` and `decorrelate_scalar` toggles instead of redefining them: no alternative plan exists for an off state to select
  - `wvlet.engine.optimizer.disabled=decorrelate_correlated` now fails with the usual "Unknown optimization" error, which lists the available names. These are debug toggles with no users outside the engine's tests
  - EngineOptimizerToggleTest iterates the toggles, so it drops them on its own
- `subqueryJoins` always binds: conjuncts with only uncorrelated scalar subqueries take its existing plain-filter path (no join; the subqueries are plan-time constants in a runtime-plan `Filter`, as before)
- Delete `RuntimePlan.Legacy` and every match on it (output, children, label, Lowering, PlanValidator, PruneColumns, PushDownFilters, ShareSubplans, Stats), `legacyConjuncts`, and `legacyOp`
- Tests that go with them: EngineDecorrelateTest's two "keep ... legacy with ... disabled" tests, EngineRuntimePlanTest's legacy-leaf test, and ShareSubplansTest's legacy-leaf case; the `legacyLeafCount` checks in EngineRuntimePlanTest no longer have a node to count and are dropped
- Next PR (recorded on the item): delete `PlannerMode.Legacy`, `wvlet.engine.planner`, `Planner.planLegacy` and the helpers only it calls, `spec/engine-plans/legacy/`, and EngineBasicSpecLegacyTest; update CLAUDE.md and the 2026-11-22 engine ADRs
  - The tests that compare the runtime engine's rows with a `legacy` engine (EngineRuntimePlanTest, EngineDecorrelateTest, EngineGroupingSetsTest) compare with DuckDB through generated SQL instead, as EngineConformance does: an independent oracle, where a fixed row list would only pin today's output. Forms DuckDB evaluates differently (e.g. `sample N`) get fixed expected rows
- Gates: spec/basic on both planners, golden plans (expected unchanged), TPC-H 22/22, TPC-DS 99/99, the decorrelate and toggle tests, engineJS/Native compile

## Alternatives and Why Not?

- Redefine the toggles' off state (e.g., correlated subqueries as an error, or as a nested-loop filter): an error option only removes queries, and a nested-loop evaluator is new execution code with no user asking for it
- Retire everything in one PR: the planner deletion touches ~1,000 lines of Planner and every test that compares against the legacy engine; keeping the Legacy-leaf removal separate keeps each diff reviewable
